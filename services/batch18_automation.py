"""
Dedicated 400-file automation engine for FlimfyBox's /batch18 pipeline.

This module is strictly isolated from SuperBatch and contains pure,
deterministic, unit-testable logic for:
  1. Up-to-400-file collection state, validation, and O(1) duplicate protection.
  2. Title/year/type grouping powered by Batch18's own smart adult parser
     (`services/batch18_metadata.py`).
  3. Deterministic episode/part/quality ordering within each title group.
  4. Retry/backoff classification for Telegram FloodWait and transient errors.
  5. Safe HTML final report generation (bounded within Telegram's 4096-char limit).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape as html_escape
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from services.batch18_metadata import (
    is_generic_channel_or_junk_title,
    normalize_fancy_unicode,
    reconcile_batch18_identity,
)

MAX_BATCH18_FILES = 400

_AUDIO_EXTENSIONS = (".mp3", ".wav", ".flac", ".aac", ".m4a", ".ogg", ".opus")

# Conservative song/promo filter: rejects explicit music tracks / lyrical clips,
# while NEVER rejecting legitimate short adult films or short OTT episodes.
_SONG_JUNK_RE = re.compile(
    r"(?i)\b(?:full\s+video\s+song|official\s+video\s+song|video\s+song|"
    r"lyrical\s+video|full\s+song|audio\s+song|jukebox|ringtone|teaser\s+promo|"
    r"official\s+trailer|official\s+teaser|motion\s+poster)\b"
)

_TITLE_KEY_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_SEASON_NUM_RE = re.compile(r"(?i)\b(?:S|SEASON)\s*0*(\d{1,2})\b")
_EPISODE_NUM_RE = re.compile(r"(?i)\b(?:EP|E|EPISODE)\s*0*(\d{1,3})\b")
_PART_NUM_RE = re.compile(r"(?i)\b(?:PART|PT|VOL)\s*0*(\d{1,3})\b")
_RES_NUM_RE = re.compile(r"(?i)\b(2160|1440|1080|720|540|480|360)p\b")


def _normalize_separators(text: str) -> str:
    return re.sub(r"[._\-]+", " ", normalize_fancy_unicode(text or "")).strip()


def is_batch18_song_junk(filename: str = "", caption: str = "") -> bool:
    """
    Return True ONLY when a file is unmistakably an audio track, video song,
    lyrical clip, trailer/teaser, or jukebox — never for short adult films/episodes.
    """
    fname = normalize_fancy_unicode(filename or "").strip()
    cap = normalize_fancy_unicode(caption or "").strip()
    lower_fname = fname.lower()
    if any(lower_fname.endswith(ext) for ext in _AUDIO_EXTENSIONS):
        return True
    norm_fname = _normalize_separators(fname)
    norm_cap = _normalize_separators(cap)
    combined = f"{norm_fname} {norm_cap}".strip()
    if not combined:
        return False
    # If filename clearly carries episode/part markers (S01E01, Part 2, Web Series),
    # treat as real video content even if a promotional caption mentions "song".
    if (
        _SEASON_NUM_RE.search(norm_fname)
        or _EPISODE_NUM_RE.search(norm_fname)
        or _PART_NUM_RE.search(norm_fname)
    ):
        return False
    return bool(_SONG_JUNK_RE.search(norm_fname) or (not norm_fname and _SONG_JUNK_RE.search(norm_cap)))


def normalize_filename_for_fingerprint(filename: str) -> str:
    text = normalize_fancy_unicode(filename or "").lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def build_batch18_file_fingerprints(
    file_unique_id: Optional[str],
    file_name: str = "",
    file_size: int = 0,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Build primary (`file_unique_id`) and secondary (`normalized_name + size`)
    deduplication keys for a collected Batch18 file.
    """
    uid = str(file_unique_id).strip() if file_unique_id else ""
    uid_key = f"uid:{uid}" if uid else None

    norm_name = normalize_filename_for_fingerprint(file_name)
    size_val = int(file_size or 0)
    meta_key = f"meta:{norm_name}:{size_val}" if (norm_name and size_val > 0) else None
    return uid_key, meta_key


def canonical_batch18_title_key(title: str) -> str:
    """Normalize a cleaned Batch18 title into a stable grouping key."""
    text = normalize_fancy_unicode(title or "").lower().strip()
    text = text.replace("&", " and ")
    key = _TITLE_KEY_NON_ALNUM_RE.sub("", text)
    return key or "unknown"


def _extract_sort_tuple(record: Dict[str, Any], original_index: int) -> Tuple[int, int, int, int, int]:
    """
    Order files inside a title group deterministically:
    Season asc -> Episode asc -> Part asc -> Resolution asc -> Original order.
    """
    extra = _normalize_separators(str(record.get("extra_info") or ""))
    fname = _normalize_separators(str(record.get("file_name") or ""))
    cap = _normalize_separators(str(record.get("caption") or ""))
    probe = f"{extra} {fname} {cap}"

    s_match = _SEASON_NUM_RE.search(probe)
    e_match = _EPISODE_NUM_RE.search(probe)
    p_match = _PART_NUM_RE.search(probe)
    r_match = _RES_NUM_RE.search(probe)

    season_num = int(s_match.group(1)) if s_match else 0
    ep_num = int(e_match.group(1)) if e_match else 0
    part_num = int(p_match.group(1)) if p_match else 0
    res_num = int(r_match.group(1)) if r_match else 9999
    return (season_num, ep_num, part_num, res_num, original_index)


def _completeness_score(record: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    """Score a record so the cleanest, richest file becomes the group's representative."""
    reconciled = record.get("reconciled") or {}
    title = str(reconciled.get("title") or record.get("detected_title") or "").strip()
    year = str(reconciled.get("year") or record.get("detected_year") or "").strip()
    platform = str(reconciled.get("platform") or record.get("platform") or "").strip()
    jav_code = str(reconciled.get("jav_code") or record.get("jav_code") or "").strip()
    needs_ai = bool(reconciled.get("needs_ai_reconcile", False))
    valid_title = 1 if (title and title != "UNKNOWN" and not is_generic_channel_or_junk_title(title)) else 0
    has_year = 1 if year else 0
    has_hint = 1 if (platform or jav_code) else 0
    confident = 1 if not needs_ai else 0
    text_len = min(len(str(record.get("caption") or "")) + len(str(record.get("file_name") or "")), 500)
    return (valid_title, confident, has_year, has_hint, text_len)


@dataclass
class Batch18CollectResult:
    accepted: bool
    reason: str  # accepted | limit_reached | duplicate | song_junk | invalid
    count: int
    max_files: int
    record: Optional[Dict[str, Any]] = None


class Batch18Collector:
    """
    Pure in-memory collector enforcing Batch18's 400-file cap, song-junk guard,
    in-batch duplicate protection, and fast Batch18 identity extraction.
    """

    def __init__(self, max_files: int = MAX_BATCH18_FILES):
        self.max_files = max(1, int(max_files))
        self.records: List[Dict[str, Any]] = []
        self.seen_uid_keys: set[str] = set()
        self.seen_meta_keys: set[str] = set()
        self.duplicates_skipped: int = 0
        self.junk_skipped: int = 0

    def add_file(
        self,
        *,
        file_id: Optional[str],
        file_unique_id: Optional[str],
        file_name: str,
        file_size: int,
        caption: str = "",
        forward_title: str = "",
        media_type: str = "document",
        source_chat_id: Optional[int] = None,
        source_message_id: Optional[int] = None,
        update_id: Optional[int] = None,
        message_obj: Any = None,
    ) -> Batch18CollectResult:
        if len(self.records) >= self.max_files:
            return Batch18CollectResult(
                accepted=False,
                reason="limit_reached",
                count=len(self.records),
                max_files=self.max_files,
            )

        clean_name = (file_name or "").strip()
        clean_caption = (caption or "").strip()
        clean_forward = (forward_title or "").strip()

        if not file_id and not file_unique_id and not clean_name and not clean_caption:
            return Batch18CollectResult(
                accepted=False,
                reason="invalid",
                count=len(self.records),
                max_files=self.max_files,
            )

        if is_batch18_song_junk(clean_name, clean_caption):
            self.junk_skipped += 1
            return Batch18CollectResult(
                accepted=False,
                reason="song_junk",
                count=len(self.records),
                max_files=self.max_files,
            )

        uid_key, meta_key = build_batch18_file_fingerprints(
            file_unique_id=file_unique_id,
            file_name=clean_name,
            file_size=file_size,
        )
        if (uid_key and uid_key in self.seen_uid_keys) or (meta_key and meta_key in self.seen_meta_keys):
            self.duplicates_skipped += 1
            return Batch18CollectResult(
                accepted=False,
                reason="duplicate",
                count=len(self.records),
                max_files=self.max_files,
            )

        reconciled = reconcile_batch18_identity(
            caption_raw=clean_caption,
            filename_raw=clean_name,
            forward_title=clean_forward,
        )

        record: Dict[str, Any] = {
            "index": len(self.records),
            "update_id": update_id,
            "source_chat_id": source_chat_id,
            "source_message_id": source_message_id,
            "media_type": media_type or "document",
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "file_name": clean_name or "File",
            "file_size": int(file_size or 0),
            "caption": clean_caption,
            "forward_title": clean_forward,
            "detected_title": reconciled["title"],
            "detected_year": reconciled["year"],
            "language": reconciled["language"] or "Hindi",
            "extra_info": reconciled["extra_info"],
            "platform": reconciled["platform"],
            "jav_code": reconciled["jav_code"],
            "content_type": reconciled["content_type"] or "Movie",
            "needs_ai_reconcile": bool(reconciled.get("needs_ai_reconcile", False)),
            "reconciled": reconciled,
            "message": message_obj,
        }

        if uid_key:
            self.seen_uid_keys.add(uid_key)
        if meta_key:
            self.seen_meta_keys.add(meta_key)
        self.records.append(record)

        return Batch18CollectResult(
            accepted=True,
            reason="accepted",
            count=len(self.records),
            max_files=self.max_files,
            record=record,
        )


def _ensure_record_reconciled(record: Dict[str, Any]) -> Dict[str, Any]:
    if record.get("reconciled") and record.get("detected_title"):
        return record
    reconciled = reconcile_batch18_identity(
        caption_raw=str(record.get("caption") or ""),
        filename_raw=str(record.get("file_name") or ""),
        forward_title=str(record.get("forward_title") or ""),
    )
    enriched = dict(record)
    enriched["reconciled"] = reconciled
    enriched["detected_title"] = reconciled["title"]
    enriched["detected_year"] = reconciled["year"]
    enriched["language"] = reconciled["language"] or record.get("language") or "Hindi"
    enriched["extra_info"] = reconciled["extra_info"]
    enriched["platform"] = reconciled["platform"]
    enriched["jav_code"] = reconciled["jav_code"]
    enriched["content_type"] = reconciled["content_type"] or "Movie"
    enriched["needs_ai_reconcile"] = bool(reconciled.get("needs_ai_reconcile", False))
    return enriched


def build_batch18_title_groups(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group collected Batch18 files into ordered per-title groups using Batch18's
    own smart adult parser/reconciler.

    Grouping guarantees:
    - Single file / single title -> 1 clean group.
    - Multiple titles (up to 400 files) -> separate groups preserving first-seen order.
    - Files with the same title and DIFFERENT explicit years (e.g. 2004 vs 2025)
      stay strictly separate.
    - A file missing a year merges into an existing same-title group when there
      is exactly ONE unambiguous year group for that title (e.g., Ep1 has (2025),
      Ep2 has no year).
    - If any file in a group is a Web Series / episode / part, the group promotes
      to `Web Series` so all parts attach to the same series title.
    - Files inside each group are sorted cleanly by Season -> Episode -> Part -> Resolution.
    """
    if not records:
        return []

    enriched_records = [_ensure_record_reconciled(r) for r in records]

    # First pass: determine which canonical titles have a single unambiguous explicit year.
    years_by_title: Dict[str, List[str]] = {}
    for rec in enriched_records:
        t_key = canonical_batch18_title_key(rec.get("detected_title") or "")
        year = str(rec.get("detected_year") or "").strip()
        if t_key == "unknown":
            continue
        bucket = years_by_title.setdefault(t_key, [])
        if year and year not in bucket:
            bucket.append(year)

    group_order: List[Tuple[str, str]] = []
    buckets: Dict[Tuple[str, str], List[Tuple[int, Dict[str, Any]]]] = {}

    for idx, rec in enumerate(enriched_records):
        raw_title = str(rec.get("detected_title") or "").strip()
        t_key = canonical_batch18_title_key(raw_title)
        year = str(rec.get("detected_year") or "").strip()

        # Unknown titles stay isolated per file so unrelated unreadable files never merge together.
        if t_key == "unknown":
            group_key = (f"unknown_{idx}", year)
        else:
            known_years = years_by_title.get(t_key, [])
            resolved_year = year
            if not resolved_year and len(known_years) == 1:
                resolved_year = known_years[0]
            group_key = (t_key, resolved_year)

        if group_key not in buckets:
            buckets[group_key] = []
            group_order.append(group_key)
        buckets[group_key].append((idx, rec))

    groups: List[Dict[str, Any]] = []
    for group_idx, g_key in enumerate(group_order, start=1):
        indexed_items = buckets[g_key]
        sorted_items = sorted(
            indexed_items,
            key=lambda pair: _extract_sort_tuple(pair[1], pair[0]),
        )
        items = [pair[1] for pair in sorted_items]

        # Pick best representative record for metadata & identity lookup
        rep = max(items, key=_completeness_score)
        rep_reconciled = rep.get("reconciled") or {}

        # Promote to Web Series if any item in the group has series/episode/part markers
        is_series = False
        platform_hint = ""
        jav_code = ""
        languages: List[str] = []
        explicit_year = g_key[1]

        for item in items:
            if str(item.get("content_type") or "").lower() == "web series":
                is_series = True
            extra = str(item.get("extra_info") or "")
            if _SEASON_NUM_RE.search(extra) or _EPISODE_NUM_RE.search(extra) or _PART_NUM_RE.search(extra):
                is_series = True
            if not platform_hint and item.get("platform"):
                platform_hint = str(item["platform"])
            if not jav_code and item.get("jav_code"):
                jav_code = str(item["jav_code"])
            lang = str(item.get("language") or "").strip()
            if lang and lang not in languages:
                languages.append(lang)
            if not explicit_year and item.get("detected_year"):
                explicit_year = str(item["detected_year"]).strip()

        group_title = str(rep_reconciled.get("title") or rep.get("detected_title") or "UNKNOWN").strip()
        group_lang = " + ".join(languages[:3]) if len(languages) > 1 else (languages[0] if languages else "Hindi")

        groups.append(
            {
                "group_index": group_idx,
                "group_key": f"{g_key[0]}|{explicit_year or 'na'}",
                "title": group_title,
                "year": explicit_year,
                "language": group_lang,
                "content_type": "Web Series" if is_series else "Movie",
                "platform": platform_hint,
                "jav_code": jav_code,
                "needs_ai_reconcile": bool(rep.get("needs_ai_reconcile", False)),
                "representative": rep,
                "files": items,
                "file_count": len(items),
            }
        )

    return groups


def is_retryable_batch18_error(exc: BaseException) -> bool:
    """
    Classify whether an exception during Telegram upload/posting or DB/provider I/O
    is transient and safe to retry with backoff.
    """
    name = type(exc).__name__.lower()
    if any(
        token in name
        for token in (
            "retryafter",
            "timedout",
            "timeout",
            "networkerror",
            "connectionerror",
            "operationalerror",
            "interfaceerror",
            "serviceunavailable",
            "deadlineexceeded",
            "toomanyrequests",
        )
    ):
        return True
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    msg = str(exc).lower()
    if any(
        phrase in msg
        for phrase in (
            "flood control",
            "retry after",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "server closed the connection",
            "bad gateway",
            "502",
            "503",
            "504",
            "429",
        )
    ):
        return True
    return False


def compute_batch18_backoff(attempt: int, retry_after: Optional[float] = None) -> float:
    """
    Compute bounded retry delay in seconds.
    Honors Telegram's `retry_after` with a small safety cushion, otherwise uses
    capped exponential backoff (1.5s, 3.0s, 6.0s, 12.0s max 20s).
    """
    if retry_after is not None:
        try:
            ra = float(retry_after)
            if ra > 0:
                return min(ra + 1.0, 65.0)
        except (TypeError, ValueError):
            pass
    step = max(1, int(attempt))
    return min(1.5 * (2 ** (step - 1)), 20.0)


@dataclass
class Batch18TitleOutcome:
    title: str
    year: str = ""
    content_type: str = "Movie"
    movie_id: Optional[int] = None
    files_total: int = 0
    files_saved: int = 0
    duplicates_skipped: int = 0
    downgrades_blocked: int = 0
    upgraded_deleted: int = 0
    files_failed: int = 0
    posted: bool = False
    post_error: str = ""
    error: str = ""


@dataclass
class Batch18RunSummary:
    batch_id: str
    is_retry: bool = False
    collected_files: int = 0
    in_batch_duplicates: int = 0
    junk_skipped: int = 0
    duration_seconds: float = 0.0
    title_outcomes: List[Batch18TitleOutcome] = field(default_factory=list)

    @property
    def titles_total(self) -> int:
        return len(self.title_outcomes)

    @property
    def titles_posted(self) -> int:
        return sum(1 for item in self.title_outcomes if item.posted)

    @property
    def titles_failed(self) -> int:
        return sum(
            1
            for item in self.title_outcomes
            if item.error or item.post_error or (item.files_failed > 0 and item.files_saved == 0)
        )

    @property
    def files_saved(self) -> int:
        return sum(item.files_saved for item in self.title_outcomes)

    @property
    def db_duplicates_skipped(self) -> int:
        return sum(item.duplicates_skipped for item in self.title_outcomes)

    @property
    def downgrades_blocked(self) -> int:
        return sum(item.downgrades_blocked for item in self.title_outcomes)

    @property
    def upgraded_deleted(self) -> int:
        return sum(item.upgraded_deleted for item in self.title_outcomes)

    @property
    def files_failed(self) -> int:
        return sum(item.files_failed for item in self.title_outcomes)


def format_batch18_final_report(summary: Batch18RunSummary, max_chars: int = 3900) -> str:
    """
    Render a clean, HTML-escaped final report for a Batch18 run (single title or
    multi-title up to 400 files), guaranteed to fit inside Telegram's 4096-char limit.
    """
    has_failures = summary.titles_failed > 0 or summary.files_failed > 0
    header_icon = "⚠️" if has_failures else "✅"
    run_label = "Batch18 Retry Report" if summary.is_retry else "Batch18 Automation Report"

    mins, secs = divmod(max(0, int(round(summary.duration_seconds))), 60)
    duration_text = f"{mins}m {secs}s" if mins else f"{secs}s"

    lines: List[str] = [
        f"{header_icon} <b>18+ {html_escape(run_label)}</b>",
        f"🆔 <b>Batch ID:</b> <code>{html_escape(summary.batch_id)}</code>",
        f"⏱️ <b>Time:</b> {html_escape(duration_text)}",
        "━━━━━━━━━━━━━━━━━━",
        f"📥 <b>Files Collected:</b> {summary.collected_files} / {MAX_BATCH18_FILES}",
        f"🎬 <b>Titles Grouped:</b> {summary.titles_total}",
        f"📢 <b>Titles Posted (/done18):</b> {summary.titles_posted} / {summary.titles_total}",
        f"💾 <b>Files Saved:</b> {summary.files_saved}",
    ]

    total_dupes = summary.in_batch_duplicates + summary.db_duplicates_skipped
    if total_dupes > 0:
        lines.append(
            f"♻️ <b>Duplicates Skipped:</b> {total_dupes} "
            f"(batch: {summary.in_batch_duplicates}, db: {summary.db_duplicates_skipped})"
        )
    if summary.junk_skipped > 0:
        lines.append(f"🎵 <b>Song/Promo Skipped:</b> {summary.junk_skipped}")
    if summary.upgraded_deleted > 0:
        lines.append(f"🔄 <b>Old Prints Upgraded:</b> {summary.upgraded_deleted}")
    if summary.downgrades_blocked > 0:
        lines.append(f"🛡️ <b>Downgrades Blocked:</b> {summary.downgrades_blocked}")
    if summary.files_failed > 0 or summary.titles_failed > 0:
        lines.append(
            f"❌ <b>Failures:</b> {summary.titles_failed} title(s), {summary.files_failed} file(s)"
        )

    footer_lines: List[str] = []
    if has_failures:
        footer_lines.append("")
        footer_lines.append("🔁 Failed items ko dobara process karne ke liye <code>/retry18</code> chalayein.")

    footer_text = "\n".join(footer_lines)

    if summary.title_outcomes:
        lines.append("")
        lines.append("<b>📋 Title Breakdown:</b>")

        rendered_count = 0
        for outcome in summary.title_outcomes:
            safe_title = html_escape(outcome.title or "Unknown")
            year_part = f" ({html_escape(outcome.year)})" if outcome.year else ""
            if outcome.error:
                icon = "❌"
                detail = f"Failed: {html_escape(outcome.error[:90])}"
            elif outcome.post_error:
                icon = "⚠️"
                detail = (
                    f"{outcome.files_saved}/{outcome.files_total} saved, "
                    f"post error: {html_escape(outcome.post_error[:70])}"
                )
            elif outcome.files_failed > 0:
                icon = "⚠️"
                detail = (
                    f"{outcome.files_saved}/{outcome.files_total} saved, "
                    f"{outcome.files_failed} failed"
                    + (" • Posted" if outcome.posted else "")
                )
            elif outcome.posted:
                icon = "✅"
                detail = f"{outcome.files_saved}/{outcome.files_total} saved • Posted"
            else:
                icon = "ℹ️"
                detail = f"{outcome.files_saved}/{outcome.files_total} saved (no new post)"

            entry = f"{icon} <b>{safe_title}</b>{year_part} — {detail}"
            candidate = "\n".join(lines + [entry]) + footer_text
            if len(candidate) > max_chars - 80:
                remaining = len(summary.title_outcomes) - rendered_count
                lines.append(f"… aur <b>{remaining}</b> aur title(s) process hue.")
                break
            lines.append(entry)
            rendered_count += 1

    text = "\n".join(lines) + footer_text
    if len(text) > max_chars:
        text = text[: max_chars - 3] + "..."
    return text
