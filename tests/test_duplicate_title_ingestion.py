from pathlib import Path


MAIN = (Path(__file__).resolve().parents[1] / "main.py").read_text(
    encoding="utf-8"
)


def test_batch_id_resolves_identity_collision_without_using_title():
    section = MAIN[
        MAIN.index("async def batch_id_command"):
        MAIN.index("async def", MAIN.index("async def batch_id_command") + 10)
    ]
    assert "_find_movie_by_provider_identity" in section
    assert "tmdb_id = await run_async" in section
    assert "if existing_movie_id:" in section
    assert "INSERT INTO movies" in section
    assert "title = %s" in section


def test_title_conflicts_are_not_used_as_an_upsert_key():
    assert "ON CONFLICT (title)" not in MAIN
    assert "def _find_movie_by_provider_identity" in MAIN


def test_duplicate_title_search_buttons_include_year_and_keep_all_rows():
    assert "ORDER BY year DESC NULLS LAST, id DESC" in MAIN
    assert "Multiple results found for" in MAIN
    assert 'button_text = f"{title}   {year_text}"' in MAIN


def test_mini_app_search_deduplicates_by_record_id_not_title():
    routes = (Path(__file__).resolve().parents[1] / "webapp_routes.py").read_text(
        encoding="utf-8"
    )
    assert "key = ('local', m['id'])" in routes
    assert "key = ('tmdb', m['id'])" in routes
