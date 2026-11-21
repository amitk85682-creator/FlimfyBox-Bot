"""Unit tests for P1 Supabase RLS remediation, P3 modular services, and API contracts."""

import sys
import types
import unittest

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    _pg_stub = types.ModuleType("psycopg2")
    _pg_stub.Error = Exception
    _pg_extras = types.ModuleType("psycopg2.extras")
    sys.modules["psycopg2"] = _pg_stub
    sys.modules["psycopg2.extras"] = _pg_extras

import db_migrations
from services.api_contracts import error_payload, health_payload, ok_payload, resolve_data_mode
from services.batch18_metadata import (
    build_imdb_suggestion_url,
    detect_ott_platform,
    pick_best_imdb_suggestion,
)
from services.rls_audit import (
    ALL_AUDITED_PUBLIC_TABLES,
    build_rls_remediation_statements,
    validate_table_identifier,
)


class RlsAuditAndMigrationTest(unittest.TestCase):
    def test_all_44_audited_tables_present_and_unique(self):
        self.assertEqual(len(ALL_AUDITED_PUBLIC_TABLES), 44)
        self.assertEqual(len(set(ALL_AUDITED_PUBLIC_TABLES)), 44)
        for required in ("movies", "movie_files", "temp_links", "global_chat_messages", "user_watchlist"):
            self.assertIn(required, ALL_AUDITED_PUBLIC_TABLES)

    def test_rls_statements_enable_rls_and_revoke_direct_roles(self):
        statements = build_rls_remediation_statements()
        self.assertEqual(len(statements), 44)
        sample = statements[0]
        self.assertIn("ENABLE ROW LEVEL SECURITY", sample)
        self.assertIn("REVOKE ALL ON TABLE", sample)
        self.assertIn("FROM anon", sample)
        self.assertIn("FROM authenticated", sample)

    def test_validate_table_identifier_blocks_sql_injection(self):
        self.assertEqual(validate_table_identifier("temp_links"), "temp_links")
        with self.assertRaises(ValueError):
            validate_table_identifier("movies; DROP TABLE users;--")

    def test_migration_21_is_registered(self):
        versions = [v for v, _ in db_migrations.MIGRATIONS]
        self.assertIn(21, versions)
        self.assertEqual(len(versions), len(set(versions)))


class Batch18MetadataServiceTest(unittest.TestCase):
    def test_imdb_suggestion_picks_matching_year_over_older_namesake(self):
        candidates = [
            {"id": "tt1493357", "l": "Dekho Magar Pyaar Se", "y": 2004, "i": {"imageUrl": "https://img/old.jpg"}},
            {"id": "tt35610472", "l": "Dekho Magar Pyar Se", "y": 2025, "i": {"imageUrl": "https://img/new.jpg"}},
        ]
        picked = pick_best_imdb_suggestion(candidates, "Dekho Magar Pyar Se", "2025")
        self.assertIsNotNone(picked)
        self.assertEqual(picked["id"], "tt35610472")

    def test_imdb_suggestion_url_and_platform_detection(self):
        url = build_imdb_suggestion_url("Dekho Magar Pyar Se")
        self.assertTrue(url.startswith("https://v3.sg.media-imdb.com/suggestion/d/"))
        self.assertEqual(
            detect_ott_platform("Dekho-Magar-Pyar-Se-2025-ULLU-Web-Series.mp4"),
            "Ullu",
        )


class ApiContractsTest(unittest.TestCase):
    def test_standard_envelopes(self):
        ok = ok_payload({"count": 3}, request_id="req-1")
        self.assertEqual(ok["status"], "success")
        self.assertEqual(ok["count"], 3)

        err = error_payload("AUTH_REQUIRED", "Sign in required", request_id="req-2")
        self.assertEqual(err, {
            "status": "error",
            "code": "AUTH_REQUIRED",
            "message": "Sign in required",
            "request_id": "req-2",
        })

        health = health_payload(service="flimfybox-bot", db_connected=True, uptime_seconds=12.345)
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["data_mode"], resolve_data_mode(True))


if __name__ == "__main__":
    unittest.main()
