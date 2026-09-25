from pathlib import Path


MAIN_SOURCE = Path(__file__).resolve().parents[1].joinpath("main.py").read_text(
    encoding="utf-8"
)


def test_fast_search_separates_indexed_lookup_from_similarity_fallback():
    search_source = MAIN_SOURCE[
        MAIN_SOURCE.index("def _get_movies_fast_sql_nocache"):
        MAIN_SOURCE.index("def get_movie_by_imdb_id", MAIN_SOURCE.index("def _get_movies_fast_sql_nocache"))
    ]
    assert "exact_sql = " in search_source
    assert "WHERE m.title ILIKE %s" in search_source
    assert "if not results:" in search_source
    assert "fuzzy_sql = " in search_source
    assert "OR SIMILARITY" not in search_source


def test_search_bounds_google_fallback_and_joins_delivery_data():
    assert "GOOGLE_SUGGESTION_TIMEOUT_SECONDS = 1.5" in MAIN_SOURCE
    assert "get_google_title_suggestions_with_timeout" in MAIN_SOURCE
    assert "def get_movie_delivery_data(movie_id)" in MAIN_SOURCE
    assert "LEFT JOIN movie_files AS mf" in MAIN_SOURCE
