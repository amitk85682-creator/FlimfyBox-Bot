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


def test_search_merges_only_by_typed_provider_identity():
    assert "local_by_tmdb" in ROUTES
    assert "local_by_title_year_type" not in ROUTES
    assert "key = (movie.get('media_type') or 'movie', str(movie['tmdb_id']))" in ROUTES
    assert "merged_tmdb_results" in ROUTES


def test_frontend_distinguishes_available_unavailable_and_upcoming_results():
    # Actual status behavior is tested in miniapp_search_theme.test.js.
    assert "FlimfyUI.availabilityLabel(movie)" in FRONTEND
    assert "function renderUnavailableAction(movie)" in FRONTEND


def test_upcoming_availability_checks_use_tmdb_identity():
    assert "m.tmdb_id = %s" in ROUTES
    assert "m.tmdb_id IS NULL" in ROUTES
    assert "NULLIF(mf.file_id, '') IS NOT NULL" in ROUTES


def test_api_keeps_namesakes_and_tmdb_movie_tv_namespaces_separate(webapp_factory, monkeypatch):
    from tests.conftest import FakeConnection
    from types import SimpleNamespace
    import webapp_routes
    rows = [
        (6111, 'Obsession', 2026, '/local.jpg', '7.8', 'Horror', 'Hollywood', 123, 'Movie', False, True),
        (16972, 'Obsession', 2025, '/series.jpg', 'N/A', 'Drama', 'Bollywood', None, 'Web Series', False, True),
        (16998, 'Obsession', 2026, None, 'N/A', 'Unknown', 'Movies', None, 'Movie', False, True),
    ]
    def get(url, **kwargs):
        if 'suggestqueries' in url:
            return SimpleNamespace(json=lambda: [None, []], raise_for_status=lambda: None)
        return SimpleNamespace(json=lambda: {'results': [
            {'id': 123, 'media_type': 'movie', 'title': 'Obsession', 'release_date': '2026-01-01', 'poster_path':'/a.jpg'},
            {'id': 123, 'media_type': 'tv', 'name': 'Obsession', 'first_air_date': '2025-01-01', 'poster_path':'/b.jpg'},
            {'id': 999, 'media_type': 'movie', 'title': 'Obsession', 'release_date': '2026-01-01', 'poster_path':'/c.jpg'},
        ]}, raise_for_status=lambda: None)
    monkeypatch.setattr(webapp_routes.requests,'get',get)
    app,_,_=webapp_factory(connection=FakeConnection(fetchall_result=rows))
    result=app.test_client().get('/api/search?q=Obsession').get_json()['results']
    assert {r['id'] for r in result if r['source']=='local'}=={6111,16972,16998}
    app,_,_=webapp_factory(connection=FakeConnection())
    result=app.test_client().get('/api/search?q=Obsession').get_json()['results']
    assert {(r['tmdb_id'],r['media_type']) for r in result}=={(123,'movie'),(123,'tv'),(999,'movie')}
    assert len({r['id'] for r in result})==3
