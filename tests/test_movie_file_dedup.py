"""Regression tests for movie_file deduplication / upsert logic.

These tests run against an in-process store that simulates the PostgreSQL
unique Telegram file identity constraints, including NULL handling, so that
no real database is needed.
"""

import importlib
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Lightweight in-memory store simulating the movie_files table
# ---------------------------------------------------------------------------

class InMemoryMovieFilesDB:
    """
    Minimal simulation of the movie_files table with:
      - auto-increment ``id``
      - UNIQUE(file_unique_id) and UNIQUE(file_id), each ignoring NULL
    """

    def __init__(self):
        self._rows: list[dict] = []
        self._next_id = 1
        self._lock = threading.Lock()

    def insert(self, row: dict):
        with self._lock:
            for identity in ("file_unique_id", "file_id"):
                value = row.get(identity)
                if value is not None and any(r.get(identity) == value for r in self._rows):
                    raise _UniqueViolation(f"UNIQUE constraint: {identity}={value}")
            row = dict(row, id=self._next_id)
            self._next_id += 1
            self._rows.append(row)

    def update(self, set_dict: dict, where: dict) -> int:
        count = 0
        for r in self._rows:
            if all(_match(r, k, v) for k, v in where.items()):
                r.update(set_dict)
                count += 1
        return count

    def upsert(self, row: dict):
        """Simulate targetless ON CONFLICT over both Telegram identity keys."""
        with self._lock:
            existing = next((
                r for r in self._rows
                if any(
                    row.get(identity) is not None
                    and r.get(identity) == row[identity]
                    for identity in ("file_unique_id", "file_id")
                )
            ), None)
            if existing:
                if existing["movie_id"] != row["movie_id"]:
                    return None
                existing.update({
                    k: v for k, v in row.items()
                    if k not in ("id", "file_unique_id", "file_id")
                })
                return existing["id"]
            for identity in ("file_unique_id", "file_id"):
                value = row.get(identity)
                if value is not None and any(r.get(identity) == value for r in self._rows):
                    raise _UniqueViolation(f"UNIQUE constraint: {identity}={value}")
            row = dict(row, id=self._next_id)
            self._next_id += 1
            self._rows.append(row)
            return row["id"]

    @property
    def all(self):
        return list(self._rows)

    def count(self):
        return len(self._rows)


class _UniqueViolation(Exception):
    pass


def _match(row, key, value):
    if value is None:
        return row.get(key) is None
    return row.get(key) == value


# ---------------------------------------------------------------------------
# Fake cursor / connection that wires into InMemoryMovieFilesDB
# ---------------------------------------------------------------------------

class FakeCursorForUpsert:
    """Parse+execute the SQL emitted by ``upsert_movie_file``."""

    def __init__(self, db: InMemoryMovieFilesDB):
        self._db = db
        self.rowcount = 0
        self._result = None

    def execute(self, sql, params=None):
        sql_norm = " ".join(sql.split()).upper()
        if "INSERT INTO MOVIE_FILES" in sql_norm and "ON CONFLICT" in sql_norm:
            row = self._build_row(params, with_fuid=True)
            self._result = self._db.upsert(row)
            self.rowcount = int(self._result is not None)
        elif "INSERT INTO MOVIE_FILES" in sql_norm:
            row = self._build_row(params, with_fuid="NULL" not in sql_norm)
            self._db.insert(row)
            self.rowcount = 1
        elif "UPDATE MOVIE_FILES" in sql_norm:
            # Figure out which UPDATE variant we're in
            if "FILE_UNIQUE_ID IS NULL" in sql_norm:
                # NULL-guard fallback UPDATE
                where = {"movie_id": params[-2], "quality": params[-1], "file_unique_id": None}
                set_dict = {
                    "file_size": params[0], "url": params[1],
                    "backup_map": params[2], "file_id": None,
                    "languages": params[3], "extra_info": params[4],
                }
            else:
                raise ValueError(f"Unexpected UPDATE SQL: {sql}")
            self.rowcount = self._db.update(set_dict, where)
        else:
            raise ValueError(f"Unexpected SQL: {sql}")

    def close(self):
        pass

    def fetchone(self):
        return (self._result,) if self._result is not None else None

    @staticmethod
    def _build_row(params, with_fuid=True):
        row = {
            "movie_id": params[0],
            "quality": params[1],
            "file_size": params[2],
            "url": params[3],
            "backup_map": params[4],
            "languages": params[5],
            "extra_info": params[6],
        }
        if with_fuid and len(params) > 7:
            row["file_unique_id"] = params[7]
            row["file_id"] = params[8] if len(params) > 8 else None
        else:
            row["file_unique_id"] = None
            row["file_id"] = None
        return row


class FakeConnForUpsert:
    def __init__(self, db: InMemoryMovieFilesDB):
        self._db = db
        self.committed = 0
        self.rolled_back = 0

    def cursor(self):
        return FakeCursorForUpsert(self._db)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


# ---------------------------------------------------------------------------
# Import the function under test
# ---------------------------------------------------------------------------

def _get_upsert_fn():
    """Import ``upsert_movie_file`` from main.py without running init code."""
    # We need a surgical import: only the function, not the full module boot.
    import importlib.util
    spec = importlib.util.spec_from_file_location("_main_mod", ROOT / "main.py",
                                                   submodule_search_locations=[])
    # Instead of actually importing (which pulls tons of deps), read the source
    # and exec only the function definition.
    source = (ROOT / "main.py").read_text(encoding="utf-8")

    # Find function boundaries
    start_marker = "\ndef upsert_movie_file("
    start = source.index(start_marker)
    # Find the next top-level def/class after it
    remaining = source[start + 1:]
    next_def = remaining.index("\ndef ")
    func_source = remaining[:next_def]

    # Provide a minimal logger
    import logging
    ns = {"logger": logging.getLogger("test_upsert")}
    exec(compile(func_source, "main.py", "exec"), ns)
    return ns["upsert_movie_file"]


upsert_movie_file = _get_upsert_fn()


# ===========================================================================
# TEST CASES
# ===========================================================================

class TestSameTelegramFileProcessedTwice:
    """Same Telegram file (same file_unique_id) processed twice → one row."""

    def test_exact_duplicate_insert(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(conn, movie_id=11965, label="1080p HDRip",
                          file_size_str="1.2 GB", main_url="https://t.me/c/1/100",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="AgACAgUAAxxx1")

        upsert_movie_file(conn, movie_id=11965, label="1080p HDRip",
                          file_size_str="1.2 GB", main_url="https://t.me/c/1/101",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="AgACAgUAAxxx1")

        assert db.count() == 1, (
            f"Expected 1 row for same file_unique_id, got {db.count()}: {db.all}"
        )

    def test_url_updated_on_reprocess(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(conn, movie_id=11965, label="720p",
                          file_size_str="800 MB", main_url="https://t.me/c/1/50",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="AgACAgUAAyyy2")

        upsert_movie_file(conn, movie_id=11965, label="720p",
                          file_size_str="800 MB", main_url="https://t.me/c/1/99",
                          backup_map_json='{"new":1}', f_lang="Hindi",
                          f_extra="", file_unique_id="AgACAgUAAyyy2")

        assert db.count() == 1
        assert db.all[0]["url"] == "https://t.me/c/1/99"
        assert db.all[0]["backup_map"] == '{"new":1}'


class TestDifferentFilesSameQualitySameSize:
    """Two genuinely different files with same quality + size → both remain."""

    def test_both_files_kept(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(conn, movie_id=11965, label="1080p HDRip",
                          file_size_str="1.4 GB", main_url="https://t.me/c/1/200",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="AAAA_unique_file_1")

        upsert_movie_file(conn, movie_id=11965, label="1080p HDRip",
                          file_size_str="1.4 GB", main_url="https://t.me/c/1/201",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="BBBB_unique_file_2")

        assert db.count() == 2, (
            "Two genuinely different files (different file_unique_id) must both "
            f"be kept. Got {db.count()}: {db.all}"
        )

    def test_episode_files_same_quality_different_fuid(self):
        """
        Web-series: S01E01 and S01E02 can have same quality.
        Both must remain as long as file_unique_ids differ.
        """
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(conn, movie_id=9999, label="720p S01E01",
                          file_size_str="400 MB", main_url="url1",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="Episode 1", file_unique_id="EP01_fuid")

        upsert_movie_file(conn, movie_id=9999, label="720p S01E02",
                          file_size_str="400 MB", main_url="url2",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="Episode 2", file_unique_id="EP02_fuid")

        assert db.count() == 2


class TestSameEpisodeSameQualityDifferentFile:
    """Same episode, same quality, but genuinely different file → do NOT delete one."""

    def test_different_encodes_of_same_episode_kept(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(conn, movie_id=9999, label="720p S01E01",
                          file_size_str="400 MB", main_url="url_encode_a",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="ENCODE_A")

        upsert_movie_file(conn, movie_id=9999, label="720p S01E01",
                          file_size_str="400 MB", main_url="url_encode_b",
                          backup_map_json="{}", f_lang="Hindi",
                          f_extra="", file_unique_id="ENCODE_B")

        assert db.count() == 2, (
            "Different encodes (different file_unique_id) of the same episode "
            "must both be kept."
        )


class TestConcurrentRetryIdempotency:
    """Concurrent/retry processing must be idempotent by Telegram identity."""

    def test_triple_insert_same_fuid(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        for _ in range(3):
            upsert_movie_file(conn, movie_id=11965, label="480p",
                              file_size_str="600 MB", main_url="url",
                              backup_map_json="{}", f_lang="", f_extra="",
                              file_unique_id="RETRY_FUID")

        assert db.count() == 1

    def test_file_id_fallback_is_idempotent(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        for _ in range(5):
            upsert_movie_file(conn, movie_id=11965, label="480p",
                              file_size_str="600 MB", main_url="url",
                              backup_map_json="{}", f_lang="", f_extra="",
                              file_unique_id=None, file_id="TG_FILE_ID")

        assert db.count() == 1, (
            f"NULL file_unique_id with the same file_id should not "
            f"create duplicates. Got {db.count()}"
        )

    def test_concurrent_retries_use_database_identity(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        def process_retry(_):
            return upsert_movie_file(
                conn, movie_id=11965, label="480p",
                file_size_str="600 MB", main_url="url",
                backup_map_json="{}", f_lang="", f_extra="",
                file_unique_id="CONCURRENT_FUID", file_id="CONCURRENT_FILE_ID",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(process_retry, range(24)))

        assert all(results)
        assert db.count() == 1

    def test_missing_telegram_identity_does_not_deduplicate_by_quality(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        for url in ("unknown_file_a", "unknown_file_b"):
            upsert_movie_file(
                conn, movie_id=11965, label="480p",
                file_size_str="600 MB", main_url=url,
                backup_map_json="{}", f_lang="", f_extra="",
                file_unique_id=None,
            )

        assert db.count() == 2

    def test_same_telegram_identity_is_not_moved_between_movies(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        upsert_movie_file(
            conn, movie_id=11965, label="480p", file_size_str="600 MB",
            main_url="url", backup_map_json="{}", f_lang="", f_extra="",
            file_unique_id="SHARED_FILE",
        )

        with pytest.raises(ValueError, match="different movie"):
            upsert_movie_file(
                conn, movie_id=11966, label="1080p", file_size_str="1 GB",
                main_url="other", backup_map_json="{}", f_lang="", f_extra="",
                file_unique_id="SHARED_FILE",
            )

        assert db.count() == 1
        assert db.all[0]["movie_id"] == 11965


class TestNullFileUniqueIdDoesNotClobberReal:
    """A NULL-fuid insert must not overwrite a row that has a real fuid."""

    def test_null_does_not_overwrite_non_null(self):
        db = InMemoryMovieFilesDB()
        conn = FakeConnForUpsert(db)

        # First: a real file
        upsert_movie_file(conn, movie_id=11965, label="720p",
                          file_size_str="800 MB", main_url="real_url",
                          backup_map_json="{}", f_lang="Hindi", f_extra="",
                          file_unique_id="REAL_FUID_123")

        # Second: a legacy insert with NULL fuid but same quality
        upsert_movie_file(conn, movie_id=11965, label="720p",
                          file_size_str="800 MB", main_url="legacy_url",
                          backup_map_json="{}", f_lang="Hindi", f_extra="",
                          file_unique_id=None)

        assert db.count() == 2, (
            "NULL-fuid row and real-fuid row are different identities; "
            "both should exist."
        )
        # Verify the real row was not overwritten
        real_rows = [r for r in db.all if r["file_unique_id"] == "REAL_FUID_123"]
        assert len(real_rows) == 1
        assert real_rows[0]["url"] == "real_url"
