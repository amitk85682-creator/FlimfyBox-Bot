import ast
import asyncio
import importlib.util
import json
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"
MAIN_TREE = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))


def _load_functions(names, namespace=None):
    selected = [
        node for node in MAIN_TREE.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in names
    ]
    env = {
        "re": re,
        "json": json,
        "time": time,
        "asyncio": asyncio,
        "GEMINI_RECONCILIATION_TIMEOUT_SECONDS": 25,
        "unicodedata": unicodedata,
        "logger": logging.getLogger("test_filename_identity"),
        "_EVIDENCE_KEYS": ("title", "year", "language", "extra_info", "category"),
    }
    if namespace:
        env.update(namespace)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(MAIN_PATH), "exec"), env)
    return env


PARSER_FUNCTIONS = {
    "clean_telegram_text",
    "normalize_episodes",
    "_extract_web_series",
    "fallback_extraction",
}
EVIDENCE_FUNCTIONS = PARSER_FUNCTIONS | {
    "_valid_evidence_title",
    "_normalize_evidence_dict",
    "_merge_csv_values",
    "_merge_extra_info",
    "_local_evidence_fallback",
    "_title_contains_filename_metadata",
    "_reconcile_filename_metadata_title",
    "reconcile_evidence_with_gemini",
}


def test_exact_drishyam_filename_is_reconciled_to_canonical_title():
    raw_filename = (
        "Drishyam: The Conclusion 2026 HQ 1080p HDTC Hindi Clean "
        "x264 HDHub4u"
    )
    model_result = {
        "title": raw_filename,
        "year": "2026",
        "language": "Hindi",
        "extra_info": "PART001",
        "category": "Web Series",
    }

    class FakeModel:
        def generate_content(self, _prompt, **_kwargs):
            return SimpleNamespace(text=json.dumps(model_result))

    class FakeGenAI:
        def configure(self, **_kwargs):
            return None

        def GenerativeModel(self, _name):
            return FakeModel()

    async def run_async(function, *args, **kwargs):
        return function(*args, **kwargs)

    env = _load_functions(
        EVIDENCE_FUNCTIONS,
        {
            "genai": FakeGenAI(),
            "get_gemini_keys": lambda: ["test-key"],
            "run_async": run_async,
        },
    )
    filename_evidence = asyncio.run(env["fallback_extraction"](raw_filename))
    reconciled = asyncio.run(
        env["reconcile_evidence_with_gemini"](
            {}, filename_evidence, filename_raw=raw_filename
        )
    )

    assert filename_evidence["title"] == "Drishyam: The Conclusion"
    assert reconciled == {
        "title": "Drishyam: The Conclusion",
        "year": "2026",
        "language": "Hindi",
        "extra_info": "",
        "category": "Movies",
    }


def test_series_ranges_and_legitimate_title_words_are_preserved():
    env = _load_functions(PARSER_FUNCTIONS)
    parse = env["fallback_extraction"]

    series = asyncio.run(parse("Reacher S02 E01-08 1080p WEB-DL"))
    numbered_title = asyncio.run(parse("2001: A Space Odyssey 1968 BluRay 1080p"))
    quality_like_title = asyncio.run(parse("The Hindi Medium 2017 1080p WEB-DL"))
    hd_title = asyncio.run(parse("The HD Movie 2024 720p"))

    assert series["title"] == "Reacher"
    assert series["category"] == "Web Series"
    assert "S02" in series["extra_info"]
    assert "E01-08" in series["extra_info"]
    assert numbered_title["title"] == "2001: A Space Odyssey"
    assert quality_like_title["title"] == "The Hindi Medium"
    assert hd_title["title"] == "The HD Movie"


class _MovieCursor:
    def __init__(self, connection):
        self.connection = connection
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))
        if "SELECT id, imdb_id, tmdb_id FROM movies" in query:
            self.connection.fetchall_result = [
                (15634, "tt27050858", 1269325)
            ]
        elif "SELECT id, year, content_type" in query:
            self.connection.fetchall_result = self.connection.title_candidates

    def fetchall(self):
        return self.connection.fetchall_result

    def fetchone(self):
        return (15634,)

    def close(self):
        return None


class _MovieConnection:
    def __init__(self, title_candidates=None):
        self.title_candidates = title_candidates or []
        self.fetchall_result = []
        self.cursor_instance = _MovieCursor(self)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _core_namespace(connection, metadata, identity_rows=None):
    async def run_async(function, *args):
        return function(*args)

    env = _load_functions(
        {"_core_movie_processor", "_optional_movie_lookup", "_set_batch_db_limits", "_find_movie_by_provider_identity",
         "_has_compatible_title_candidate"},
        {
            "run_async": run_async,
            "fetch_movie_metadata": lambda *args: metadata,
            "normalize_catalog_labels": lambda **_kwargs: ("Bollywood", "Movie"),
            "fetch_cast_from_imdb": lambda *_args: "cast",
            "resolve_trailer_key": lambda *_args: None,
            "fetch_tmdb_artwork": lambda *_args: (None, None),
            "resolve_tmdb_id_from_imdb": (
                lambda imdb_id, *_args: 1269325 if imdb_id else None
            ),
            "get_db_connection": lambda: connection,
            "close_db_connection": lambda _conn: None,
        },
    )
    if identity_rows is not None:
        connection.fetchall_result = identity_rows
    return env


def test_core_reuses_provider_identity_and_saves_file_under_canonical_id():
    connection = _MovieConnection()
    metadata = (
        "Drishyam: The Conclusion", 2026, None, "Crime, Drama",
        "tt27050858", "7.4", "Canonical plot", "Bollywood", {},
    )
    env = _core_namespace(
        connection,
        metadata,
        identity_rows=[(15634, "tt27050858", 1269325)],
    )
    reconciled = {
        "title": "Drishyam: The Conclusion",
        "year": "2026",
        "language": "Hindi",
        "extra_info": "",
        "category": "Movies",
    }
    result = asyncio.run(
        env["_core_movie_processor"](
            "Drishyam: The Conclusion 2026 HQ 1080p HDTC Hindi Clean x264 HDHub4u",
            reconciled_data=reconciled,
        )
    )

    assert result["movie_id"] == 15634
    statements = [query for query, _params in connection.cursor_instance.executed]
    assert any("UPDATE movies" in query for query in statements)
    assert not any("INSERT INTO movies" in query for query in statements)

    helpers_path = ROOT / "tests" / "test_movie_file_dedup.py"
    spec = importlib.util.spec_from_file_location(
        "_movie_file_dedup_test_helpers", helpers_path
    )
    helpers = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = helpers
    spec.loader.exec_module(helpers)
    files_db = helpers.InMemoryMovieFilesDB()
    file_conn = helpers.FakeConnForUpsert(files_db)
    helpers.upsert_movie_file(
        file_conn,
        movie_id=result["movie_id"],
        label="1080p HDTC",
        file_size_str="3.90 GB",
        main_url="telegram://file",
        backup_map_json="{}",
        f_lang="Hindi",
        f_extra="",
        file_unique_id="DRISHYAM_FILE",
    )
    helpers.upsert_movie_file(
        file_conn,
        movie_id=result["movie_id"],
        label="1080p HDTC",
        file_size_str="3.90 GB",
        main_url="telegram://file-retry",
        backup_map_json="{}",
        f_lang="Hindi",
        f_extra="",
        file_unique_id="DRISHYAM_FILE",
    )

    assert len(files_db.all) == 1
    assert files_db.all[0]["movie_id"] == 15634


def test_core_holds_unresolved_exact_title_instead_of_creating_duplicate():
    connection = _MovieConnection(title_candidates=[(15634, 2026, "Movie")])
    env = _core_namespace(connection, metadata=None)
    reconciled = {
        "title": "Drishyam: The Conclusion",
        "year": "2026",
        "language": "Hindi",
        "extra_info": "",
        "category": "Movies",
    }

    result = asyncio.run(
        env["_core_movie_processor"](
            "Drishyam: The Conclusion 2026 HQ 1080p HDTC Hindi Clean x264 HDHub4u",
            reconciled_data=reconciled,
        )
    )

    assert result is None
    assert not any(
        "INSERT INTO movies" in query
        for query, _params in connection.cursor_instance.executed
    )
    assert connection.commits == 0
