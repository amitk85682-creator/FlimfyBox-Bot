"""Reliability primitives for the Python Telegram bot.

Provides:
- Thread-safe, bounded TTL caching (`ThreadSafeFastCache`) so concurrent
  interactive/background threads never race or grow memory unboundedly.
- Tiered DB connection admission (`TieredConnectionGate`) so background workers
  (metadata refresh, auto-delete, chat relay, telemetry) can never starve
  interactive Telegram search/file delivery or Mini App web requests.
- Safe search-cache wrapper (`cached_search_lookup`) that refuses to cache
  empty results caused by DB pool timeouts or transient SQL failures.
- Unified temporary download token validation (`is_valid_temp_link_token`)
  accepting both Python (`tmp_` + 12 hex) and Node (`tmp_` + 16 hex) tokens.
- File extra-info & season-year formatting (`format_file_extra_display`).
"""

from __future__ import annotations

import re
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Optional, Tuple


TEMP_LINK_TOKEN_RE = re.compile(r"^tmp_[0-9a-f]{12,32}$")

_EDITION_KEYWORDS = (
    "UNCUT",
    "EXTENDED",
    "CUT",
    "UNRATED",
    "REMASTERED",
    "EDITION",
)


def is_valid_temp_link_token(payload: Optional[str]) -> bool:
    """Validate short-lived /start tmp_<hex> tokens issued by Python or Node."""
    if not isinstance(payload, str):
        return False
    return bool(TEMP_LINK_TOKEN_RE.fullmatch(payload))


class ThreadSafeFastCache:
    """Thread-safe TTL cache with bounded LRU eviction."""

    def __init__(self, ttl_seconds: float = 3600, max_entries: int = 1024):
        self.ttl = max(1.0, float(ttl_seconds))
        self.max_entries = max(16, int(max_entries))
        self.cache: OrderedDict[Any, Tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Any) -> Any:
        now = time.time()
        with self._lock:
            entry = self.cache.get(key)
            if entry is None:
                return None
            data, timestamp = entry
            if now - timestamp < self.ttl:
                self.cache.move_to_end(key)
                return data
            self.cache.pop(key, None)
            return None

    def set(self, key: Any, value: Any) -> None:
        now = time.time()
        with self._lock:
            self.cache[key] = (value, now)
            self.cache.move_to_end(key)
            while len(self.cache) > self.max_entries:
                self.cache.popitem(last=False)

    def delete(self, key: Any) -> None:
        with self._lock:
            self.cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self.cache.clear()


class TieredConnectionGate:
    """Admission gate keeping interactive bot & web DB slots safe from background workers.

    Roles:
      - 'web': acquires total capacity only (protected by web_reserved slots).
      - 'bot': acquires bot capacity (total - web_reserved), then total capacity.
      - 'background': acquires background capacity (capped small, e.g. 2 slots),
        then bot capacity, then total capacity.
    """

    def __init__(
        self,
        max_connections: int = 16,
        web_reserved: int = 3,
        background_max: int = 2,
    ):
        self.max_connections = max(4, int(max_connections))
        self.web_reserved = max(1, min(int(web_reserved), self.max_connections - 2))
        self.bot_max = max(2, self.max_connections - self.web_reserved)
        self.background_max = max(1, min(int(background_max), max(1, self.bot_max - 2)))

        self._total_sem = threading.BoundedSemaphore(self.max_connections)
        self._bot_sem = threading.BoundedSemaphore(self.bot_max)
        self._bg_sem = threading.BoundedSemaphore(self.background_max)
        self._lease_lock = threading.Lock()
        self._leases: dict[int, str] = {}

    def acquire(self, role: str, timeout: float) -> bool:
        """Acquire admission slots for `role` ('web', 'bot', or 'background')."""
        if role not in ("web", "bot", "background"):
            raise ValueError(f"Unsupported DB pool role: {role!r}")

        bg_acquired = False
        bot_acquired = False

        if role == "background":
            bg_acquired = self._bg_sem.acquire(timeout=timeout)
            if not bg_acquired:
                return False

        if role in ("bot", "background"):
            bot_acquired = self._bot_sem.acquire(timeout=timeout)
            if not bot_acquired:
                if bg_acquired:
                    self._bg_sem.release()
                return False

        if not self._total_sem.acquire(timeout=timeout):
            if bot_acquired:
                self._bot_sem.release()
            if bg_acquired:
                self._bg_sem.release()
            return False

        return True

    def release_unregistered(self, role: str) -> None:
        """Release admission slots when checkout fails before registering a connection."""
        self._total_sem.release()
        if role in ("bot", "background"):
            self._bot_sem.release()
        if role == "background":
            self._bg_sem.release()

    def register_lease(self, conn_id: int, role: str) -> int:
        with self._lease_lock:
            self._leases[conn_id] = role
            return len(self._leases)

    def release_lease(self, conn_id: int) -> Optional[str]:
        """Remove a lease record; caller releases slots via release_role() after putconn."""
        with self._lease_lock:
            return self._leases.pop(conn_id, None)

    def release_role(self, role: Optional[str]) -> None:
        if role is None:
            return
        self._total_sem.release()
        if role in ("bot", "background"):
            self._bot_sem.release()
        if role == "background":
            self._bg_sem.release()

    def active_count(self) -> int:
        with self._lease_lock:
            return len(self._leases)


def cached_search_lookup(
    cache: Any,
    cache_key: str,
    fetch_fn: Callable[[], Tuple[list, bool]],
) -> Tuple[list, bool]:
    """Run a search lookup with TTL caching that NEVER caches DB failures.

    `fetch_fn()` must return `(rows, db_ok)`.
    Returns `(rows, db_ok)`. When `db_ok` is False, the cache is NOT populated,
    preventing transient pool timeouts from poisoning subsequent searches.
    """
    cached = cache.get(cache_key)
    if cached is not None:
        return cached, True

    rows, db_ok = fetch_fn()
    if db_ok:
        cache.set(cache_key, rows)
    return rows, db_ok


def format_file_extra_display(
    extra_val: Optional[str],
    seasons_data_db: Optional[dict] = None,
    current_year_html: str = "",
) -> Tuple[str, str]:
    """Format extra_info caption line and refine season/episode year when available."""
    cleaned = str(extra_val or "").strip()
    if not cleaned:
        return "", current_year_html

    ext = cleaned.upper()
    if any(word in ext for word in _EDITION_KEYWORDS):
        extra_display = f"📌 <b>Edition:</b> {cleaned}\n"
    elif "S" in ext and "E" in ext:
        extra_display = f"📌 <b>Season & Episode:</b> {cleaned}\n"
    elif "S" in ext:
        extra_display = f"📌 <b>Season:</b> {cleaned}\n"
    elif "E" in ext:
        extra_display = f"📌 <b>Episode:</b> {cleaned}\n"
    else:
        extra_display = f"📌 <b>Info:</b> {cleaned}\n"

    year_html = current_year_html
    if ("S" in ext or "SEASON" in ext) and isinstance(seasons_data_db, dict):
        s_match = re.search(r"(?i)(?:S|SEASON\s*)0*(\d+)", ext)
        e_match = re.search(r"(?i)(?:E|EPISODE\s*)0*(\d+)", ext)
        if s_match:
            s_num = str(int(s_match.group(1)))
            if s_num in seasons_data_db:
                s_data = seasons_data_db[s_num] or {}
                specific_year = s_data.get("year") or (
                    s_data.get("air_date", "")[:4] if s_data.get("air_date") else None
                )
                if e_match and isinstance(s_data.get("episodes"), dict):
                    e_num = str(int(e_match.group(1)))
                    ep_info = s_data["episodes"].get(e_num)
                    if isinstance(ep_info, dict) and ep_info.get("air_date"):
                        specific_year = str(ep_info["air_date"])[:4]
                if specific_year and str(specific_year).isdigit() and int(specific_year) > 0:
                    year_html = f"📅 <b>Year:</b> {specific_year}\n"

    return extra_display, year_html
