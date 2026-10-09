from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")
FRONTEND = (ROOT / "static" / "miniapp" / "app.js").read_text(encoding="utf-8")


def test_search_exposes_local_file_availability_and_tmdb_identity():
    assert "AS is_available" in ROUTES
    assert "'tmdb_id': r[7]" in ROUTES
    assert "'availability_state': 'available'" in ROUTES
    assert "else 'unavailable'" in ROUTES
    assert "'tmdb_id': item.get('id')" in ROUTES


def test_search_merges_by_provider_identity_before_title_year_fallback():
    assert "local_by_tmdb" in ROUTES
    assert "local_by_title_year_type" in ROUTES
    assert "Title/year/type is only safe when it identifies one local row." in ROUTES
    assert "merged_tmdb_results" in ROUTES


def test_frontend_distinguishes_available_unavailable_and_upcoming_results():
    assert "const isUpcoming = Boolean(r.is_upcoming);" in FRONTEND
    assert "const isAvailable = Boolean(r.is_available);" in FRONTEND
    assert "const status = isUpcoming ? 'Upcoming' : (isAvailable ? 'Available' : 'Unavailable');" in FRONTEND
    assert "function renderUnavailableAction(movie)" in FRONTEND


def test_upcoming_availability_checks_use_tmdb_identity():
    assert "m.tmdb_id = %s" in ROUTES
    assert "m.tmdb_id IS NULL" in ROUTES
    assert "NULLIF(mf.file_id, '') IS NOT NULL" in ROUTES
