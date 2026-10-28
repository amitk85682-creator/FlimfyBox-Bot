import ast
import asyncio
import __future__
import logging
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "main.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _load(*names, **extra):
    nodes = [
        node for node in TREE.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in names
    ]
    namespace = {
        "re": re,
        "logger": logging.getLogger("test_quality_cleanup"),
        "_SOURCE_LEVELS": {
            1: [
                "cam", "camrip", "hdcam", "hd-cam", "hqcam", "hq-cam",
                "telecine", "tc", "telesync", "ts", "hallprint",
                "hall-print", "hall print", "screen recording",
                "screen-recording", "recording",
            ],
            2: [
                "hdts", "hd-ts", "predvd", "pre-dvd", "dvdscr",
                "dvdscreener", "scr", "screener", "line", "line audio",
                "hdtc", "hd-tc", "hq-hdtc",
            ],
            3: ["hdrip", "webrip", "web-rip", "hc-webrip", "hdtv"],
            4: [
                "web-dl", "webdl", "bluray", "blu-ray", "bdrip",
                "brrip", "ds4k", "remux",
            ],
        },
    }
    namespace["_KEYWORD_TO_LEVEL"] = {
        keyword: level
        for level, keywords in namespace["_SOURCE_LEVELS"].items()
        for keyword in keywords
    }
    namespace.update(extra)
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "main.py",
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        namespace,
    )
    return namespace


class _CleanupCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0

    def execute(self, query, params=None):
        normalized = " ".join(query.split()).upper()
        if normalized.startswith("SELECT ID, QUALITY, EXTRA_INFO, LANGUAGES"):
            self.connection.result = list(self.connection.rows)
        elif normalized.startswith("DELETE FROM MOVIE_FILES"):
            deleted_ids = set(params[1:])
            self.connection.deleted.extend(sorted(deleted_ids))
            self.connection.rows = [
                row for row in self.connection.rows if row[0] not in deleted_ids
            ]
            self.rowcount = len(deleted_ids)
        else:
            raise AssertionError(query)

    def fetchall(self):
        return self.connection.result

    def close(self):
        pass


class _CleanupConnection:
    def __init__(self, rows):
        self.rows = list(rows)
        self.result = []
        self.deleted = []
        self.commits = 0

    def cursor(self):
        return _CleanupCursor(self)

    def commit(self):
        self.commits += 1


def _quality_namespace():
    return _load(
        "get_source_level", "get_resolution", "get_file_content_scope",
        "_content_scope_is_replaced", "_language_set",
        "_legacy_source_is_replaced", "auto_upgrade_delete",
    )


def test_dirty_source_aliases_and_contradictory_labels_stay_dirty():
    get_level = _quality_namespace()["get_source_level"]
    assert get_level("720p HallPrint") == 1
    assert get_level("1080p Screen Recording") == 1
    assert get_level("CamRip HDRip") == 1
    assert get_level("1080p HDTS") == 2
    assert get_level("1080p WEB-DL") == 4


def test_quality_label_normalizes_hallprint_and_recording_sources():
    namespace = _load(
        "generate_quality_label", normalize_episodes=lambda value: value
    )
    generate = namespace["generate_quality_label"]
    assert generate("Movie.720p.HallPrint.mkv") == "720p CAMRip"
    assert generate("Movie.1080p.Screen.Recording.mkv") == "1080p CAMRip"


def test_full_digital_movie_removes_split_prints_and_safe_legacy_rows():
    cleanup = _quality_namespace()["auto_upgrade_delete"]
    connection = _CleanupConnection([
        (1, "1080p HallPrint", "PART001", "Hindi"),
        (2, "1080p HDTS", "PART002", "Hindi"),
        (3, "1080p", "", "Hindi"),
        (4, "1080p", "", "Tamil"),
        (5, "1080p WEB-DL", "", "Hindi"),
    ])

    deleted, _labels = cleanup(
        6815, "1080p WEB-DL", "", connection, new_languages="Hindi"
    )

    assert deleted == 3
    assert connection.deleted == [1, 2, 3]
    assert {row[0] for row in connection.rows} == {4, 5}


def test_digital_episode_range_only_removes_the_ranges_it_covers():
    cleanup = _quality_namespace()["auto_upgrade_delete"]
    connection = _CleanupConnection([
        (1, "720p CAMRip", "S02 E01-04 COMBINED", "Hindi"),
        (2, "720p CAMRip", "S02 E07-12 COMBINED", "Hindi"),
        (3, "1080p WEB-DL", "S02 E01-06 COMBINED", "Hindi"),
    ])

    deleted, _labels = cleanup(
        4710, "1080p WEB-DL", "S02 E01-06 COMBINED", connection,
        new_languages="Hindi",
    )

    assert deleted == 1
    assert connection.deleted == [1]


def test_movie_part_does_not_turn_filename_into_web_series_title():
    async def fail_web_series(*_args):
        raise AssertionError("Part-only movie must not enter series parsing")

    namespace = _load(
        "fallback_extraction",
        clean_telegram_text=lambda text: text,
        _extract_web_series=fail_web_series,
    )
    result = asyncio.run(namespace["fallback_extraction"](
        "Spider.Man.Brand.New.Day.2026.1080p.TELESYNC.ENG.HIN.Part.1.mkv"
    ))

    assert result["title"] == "Spider Man Brand New Day"
    assert result["year"] == "2026"
    assert result["category"] == "Movies"
    assert result["extra_info"] == "PART001"
