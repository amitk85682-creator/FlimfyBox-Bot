from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")
MIGRATIONS = (ROOT / "db_migrations.py").read_text(encoding="utf-8")
FRONTEND = (ROOT / "static" / "miniapp" / "app.js").read_text(encoding="utf-8")


def test_notification_schema_has_independent_stages():
    assert "release_notification_requested BOOLEAN" in MIGRATIONS
    assert "availability_notification_requested BOOLEAN" in MIGRATIONS
    assert "release_notified_at TIMESTAMP" in MIGRATIONS
    assert "availability_notified_at TIMESTAMP" in MIGRATIONS


def test_worker_checks_release_and_actual_local_file_separately():
    assert "release_notification_requested" in MAIN
    assert "availability_notification_requested" in MAIN
    assert "release_notified_at = CURRENT_TIMESTAMP" in MAIN
    assert "availability_notified_at = CURRENT_TIMESTAMP" in MAIN
    assert "JOIN movie_files mf ON mf.movie_id = m.id" in MAIN


def test_api_and_frontend_support_two_notification_stages():
    assert "stage = (payload.get('stage')" in ROUTES
    assert "'availability_state': state" in ROUTES
    assert "data-stage" in FRONTEND
    assert "Notify when available for download" in FRONTEND
    assert "stage: button.dataset.stage" in FRONTEND
