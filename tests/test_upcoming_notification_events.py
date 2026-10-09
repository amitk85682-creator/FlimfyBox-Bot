from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")


def test_release_and_availability_events_have_independent_state_columns():
    worker = MAIN[MAIN.index("async def upcoming_reminder_worker"):]
    assert "release_notified_at IS NULL" in worker
    assert "availability_notified_at IS NULL" in worker
    assert "SET release_notified_at = CURRENT_TIMESTAMP" in worker
    assert "SET availability_notified_at = CURRENT_TIMESTAMP" in worker


def test_release_message_does_not_promise_a_download():
    worker = MAIN[MAIN.index("async def upcoming_reminder_worker"):]
    release_block = worker.split("if availability_requested and local_available:", 1)[0]
    assert "Download will be available separately." in release_block
    assert "Open FlimfyBox:" not in release_block
    assert "Download Now:" not in release_block


def test_availability_requires_a_real_root_or_movie_file_and_uses_tmdb_identity():
    worker = MAIN[MAIN.index("async def upcoming_reminder_worker"):]
    assert "m.tmdb_id = %s" in worker
    assert "NULLIF(m.url, '') IS NOT NULL" in worker
    assert "NULLIF(m.file_id, '') IS NOT NULL" in worker
    assert "NULLIF(mf.url, '') IS NOT NULL" in worker
    assert "NULLIF(mf.file_id, '') IS NOT NULL" in worker
    assert "The download is now ready on FlimfyBox." in worker
    assert "Download Now:" not in worker
    assert "Open FlimfyBox:" not in worker


def test_availability_subscription_is_not_blocked_by_official_release_date():
    availability_block = ROUTES[
        ROUTES.index("if stage == 'availability':"):
        ROUTES.index("cur.execute(", ROUTES.index("if stage == 'availability':"))
    ]
    assert "Availability notification starts after release." not in availability_block
