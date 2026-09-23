from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")
FRONTEND = (ROOT / "static" / "miniapp" / "app.js").read_text(encoding="utf-8")
MIGRATIONS = (ROOT / "db_migrations.py").read_text(encoding="utf-8")
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")


def test_upcoming_notifications_are_persistent_and_unique():
    assert "CREATE TABLE IF NOT EXISTS upcoming_notifications" in MIGRATIONS
    assert "UNIQUE (user_id, tmdb_id)" in MIGRATIONS
    assert "notified_at TIMESTAMP NULL" in MIGRATIONS
    assert "INSERT INTO upcoming_notifications" in ROUTES
    assert "ON CONFLICT (user_id, tmdb_id)" in ROUTES


def test_notification_endpoint_uses_authenticated_telegram_identity():
    route = ROUTES[ROUTES.index("def toggle_upcoming_reminder") :]
    assert "user, error = require_telegram_user()" in route
    assert "payload.get('user_id')" not in route
    assert "request.args.get('user_id')" not in route


def test_upcoming_rating_is_locked_and_post_is_rejected():
    assert "if movie_row[0]:" in ROUTES
    assert "'rating_locked': True" in ROUTES
    assert "Rating is available after release." in ROUTES
    assert "movie['is_upcoming'] = bool(row[14])" in ROUTES


def test_upcoming_details_use_notify_and_locked_rating_not_request():
    assert "renderUpcomingActions(movie)" in FRONTEND
    assert "Expected release:" in FRONTEND
    assert "renderLockedCommunityRating(movie.title || 'this title')" in FRONTEND
    assert "document.getElementById('dpTrailerBtn').innerHTML = `<button class=\"btn-request\"" not in FRONTEND
    assert "toggleUpcomingNotification" in FRONTEND
    assert "Rating available after release" in FRONTEND


def test_upcoming_cards_keep_the_shared_horizontal_card_renderer():
    assert "renderCards(upcoming, 'card', true)" in FRONTEND
    assert "ensureHomeSectionRow('rowUpcoming'" in FRONTEND


def test_worker_tracks_notification_stages_separately():
    worker = MAIN[MAIN.index("async def upcoming_reminder_worker") :]
    assert "FROM upcoming_notifications" in worker
    assert "release_notified_at IS NULL" in worker
    assert "availability_notified_at IS NULL" in worker
    assert "SET release_notified_at = CURRENT_TIMESTAMP" in worker
    assert "SET availability_notified_at = CURRENT_TIMESTAMP" in worker
    assert "is now released!" in worker


def test_upcoming_endpoint_excludes_titles_releasing_today():
    assert "'primary_release_date.gte': (today + timedelta(days=1)).isoformat()" in ROUTES
    assert "'first_air_date.gte': (today + timedelta(days=1)).isoformat()" in ROUTES
    assert "date() > today" in ROUTES


def test_existing_local_rating_path_remains_available():
    assert "result = rating_summary(cur, movie_id, user['id'])" in ROUTES
    assert "movie['is_available'] = True" in ROUTES
