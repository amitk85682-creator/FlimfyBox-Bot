from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = (ROOT / "db_migrations.py").read_text(encoding="utf-8")
ROUTES = (ROOT / "webapp_routes.py").read_text(encoding="utf-8")
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")
FRONTEND = (ROOT / "static" / "miniapp" / "app.js").read_text(encoding="utf-8")


def test_watchlist_availability_alerts_have_opt_in_state_and_due_index():
    migration = MIGRATIONS[MIGRATIONS.index("def _migration_17") :]
    assert "availability_notification_requested BOOLEAN NOT NULL DEFAULT FALSE" in migration
    assert "availability_notified_at TIMESTAMP NULL" in migration
    assert "availability_notification_lease_until TIMESTAMP NULL" in migration
    assert "idx_watchlist_availability_due" in migration


def test_my_list_alert_endpoint_uses_telegram_identity_and_requires_a_boolean():
    endpoint = ROUTES[
        ROUTES.index("def toggle_my_list_availability_notification") :
        ROUTES.index("# 🤖 GOOGLE AUTO-SUGGEST PROXY", ROUTES.index("def toggle_my_list_availability_notification"))
    ]
    assert "user, error = require_telegram_user()" in endpoint
    assert "not isinstance(payload.get('enabled'), bool)" in endpoint
    assert "availability_notification_requested = TRUE" in endpoint
    assert "local_movie_has_file_sql('m')" in endpoint
    assert "already_available" in endpoint


def test_availability_worker_claims_once_and_retries_with_backoff():
    worker = MAIN[MAIN.index("async def upcoming_reminder_worker") :]
    assert "_claim_pending_watchlist_availability_notifications" in worker
    assert "_mark_watchlist_availability_notified" in worker
    assert "_retry_watchlist_availability_notification" in worker
    claim = worker[worker.index("def _claim_pending_watchlist_availability_notifications") :]
    assert "FOR UPDATE OF w SKIP LOCKED" in claim
    assert "availability_notification_lease_until = CURRENT_TIMESTAMP + INTERVAL '5 minutes'" in claim
    assert "availability_notified_at = CURRENT_TIMESTAMP" in claim
    assert "INTERVAL '6 hours'" in claim


def test_local_unavailable_title_has_a_working_save_and_notify_control():
    assert "toggleWatchlistAvailabilityNotification(this)" in FRONTEND
    assert "/availability-notification" in FRONTEND
    assert "Save &amp; notify me" in FRONTEND
    assert "aria-pressed=\"false\"" in FRONTEND
