import ast
import logging
import os
import re
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"
MAIN_TREE = ast.parse(MAIN_PATH.read_text(encoding="utf-8"))


def _load_main_functions(names, extra_globals=None):
    nodes = [
        node for node in MAIN_TREE.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in names
    ]
    namespace = {
        "os": os,
        "re": re,
        "quote": quote,
        "TMDB_API_KEY": "synthetic-test-key",
        "logger": logging.getLogger("test_metadata_category"),
    }
    if extra_globals:
        namespace.update(extra_globals)
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), str(MAIN_PATH), "exec"),
        namespace,
    )
    return namespace


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class _ProviderRequests:
    def __init__(self, omdb_payload):
        self.omdb_payload = omdb_payload

    def get(self, url, **_kwargs):
        if "omdbapi.com" in url:
            return _Response(self.omdb_payload)
        if "/find/" in url:
            return _Response({
                "movie_results": [{"poster_path": "/provider-poster.jpg"}],
                "tv_results": [],
            })
        raise AssertionError("Unexpected provider request")


def _fetch_by_imdb(omdb_payload, monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "synthetic-test-key")
    env = _load_main_functions(
        {"fetch_movie_metadata", "normalize_artwork_url"},
        {"requests": _ProviderRequests(omdb_payload)},
    )
    return env["fetch_movie_metadata"]("tt34866681")


def _omdb_payload(*, country, language, imdb_id="tt34866681"):
    return {
        "Response": "True",
        "Title": "Lucky",
        "Year": "2026",
        "Genre": "Crime, Drama, Thriller",
        "imdbRating": "7.0",
        "Plot": "A provider-metadata classification test.",
        "imdbID": imdb_id,
        "Country": country,
        "Language": language,
        "Type": "movie",
        "Poster": "N/A",
    }


def _normalize_provider_category(provider_category, batch_language="Hindi"):
    env = _load_main_functions({"normalize_catalog_labels"})
    return env["normalize_catalog_labels"](
        category=provider_category,
        language=batch_language,
        genre="Crime, Drama, Thriller",
        title="Lucky",
    )[0]


def test_lucky_uses_provider_country_and_is_not_reclassified_by_batch_language(monkeypatch):
    metadata = _fetch_by_imdb(
        _omdb_payload(country="United States", language="English"), monkeypatch
    )

    assert metadata[0:2] == ("Lucky", 2026)
    assert metadata[3] == "Crime, Drama, Thriller"
    assert metadata[4] == "tt34866681"
    assert metadata[7] == "Hollywood"
    assert _normalize_provider_category(metadata[7]) == "Hollywood"


def test_indian_hindi_provider_metadata_remains_bollywood(monkeypatch):
    metadata = _fetch_by_imdb(
        _omdb_payload(country="India", language="Hindi"), monkeypatch
    )

    assert metadata[7] == "Bollywood"
    assert _normalize_provider_category(metadata[7]) == "Bollywood"


def test_indian_tamil_provider_metadata_remains_south(monkeypatch):
    metadata = _fetch_by_imdb(
        _omdb_payload(country="India", language="Tamil"), monkeypatch
    )

    assert metadata[7] == "South"
    assert _normalize_provider_category(metadata[7]) == "South"


def test_missing_provider_origin_stays_neutral_despite_hindi_batch_default(monkeypatch):
    metadata = _fetch_by_imdb(
        _omdb_payload(country="", language=""), monkeypatch
    )

    assert metadata[7] == "Movies"
    assert _normalize_provider_category(metadata[7]) == "Movies"


def test_animation_genre_is_not_automatically_mislabeled_as_anime():
    env = _load_main_functions({"normalize_catalog_labels"})
    category, content_type = env["normalize_catalog_labels"](
        category="Hollywood",
        content_type="Movie",
        genre="Animation, Adventure, Family",
        title="Toy Story",
    )
    assert (category, content_type) == ("Hollywood", "Movie")


def test_anime_region_keeps_real_movie_or_series_content_type():
    env = _load_main_functions({"normalize_catalog_labels"})
    normalize = env["normalize_catalog_labels"]
    assert normalize(category="Anime", title="Your Name") == ("Anime", "Movie")
    assert normalize(category="Anime", extra_info="S01 E03") == (
        "Anime", "Web Series"
    )


def test_regional_series_signal_preserves_region_and_series_type(monkeypatch):
    payload = _omdb_payload(country="India", language="Hindi")
    payload["Type"] = "series"
    metadata = _fetch_by_imdb(payload, monkeypatch)
    assert metadata[7] == "Bollywood Web Series"
    env = _load_main_functions({"normalize_catalog_labels"})
    assert env["normalize_catalog_labels"](category=metadata[7]) == (
        "Bollywood", "Web Series"
    )


def test_batchid_response_uses_normalized_metadata_category():
    source = MAIN_PATH.read_text(encoding="utf-8")
    batchid = source[
        source.index("async def batch_id_command"):
        source.index(
            "async def batch_add_command",
            source.index("async def batch_id_command"),
        )
    ]

    assert "category, content_type = normalize_catalog_labels(" in batchid
    assert "f\"🏷️ **Category:** {category}\\n\"" in batchid
