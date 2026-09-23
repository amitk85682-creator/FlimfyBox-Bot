from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")
FRONTEND = (ROOT / "static" / "miniapp" / "app.js").read_text(encoding="utf-8")
MIGRATIONS = (ROOT / "db_migrations.py").read_text(encoding="utf-8")


def test_content_type_column_and_index_are_migrated():
    assert "ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'Movie'" in MIGRATIONS
    assert "idx_movies_content_type" in MIGRATIONS


def test_ingestion_normalizes_region_and_media_type():
    assert "def normalize_catalog_labels(" in MAIN
    assert 'media_type = "Anime"' in MAIN
    assert 'media_type = "Web Series"' in MAIN
    assert 'media_type = "Movie"' in MAIN
    assert 'region = "Bollywood"' in MAIN
    assert 'region = "Hollywood"' in MAIN
    assert "content_type = COALESCE(%s, content_type)" in MAIN


def test_api_exposes_content_type_and_browse_uses_it():
    assert "COALESCE(content_type, 'Movie') as content_type" in ROUTES
    assert "def is_catalogue_tv(category, seasons_data, content_type=None)" in ROUTES
    assert "str(content_type or '').strip().lower() in {'web series', 'tv series'}" in ROUTES
    assert "'content_type': r[9]" in ROUTES


def test_home_rows_consider_content_type():
    assert "const contentType = String(movie.content_type || '').toLowerCase();" in FRONTEND
    assert "movie.content_type || ''" in FRONTEND
