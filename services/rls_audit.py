"""
Authoritative Supabase PostgreSQL Row-Level Security (RLS) inventory and
idempotent remediation generator for FlimfyBox's public schema.

All tables in FlimfyBox are accessed strictly through the backend service role
(`postgres` / `service_role` via DATABASE_URL). Direct PostgREST Data API access
for `anon` and `authenticated` roles is explicitly revoked while RLS is enforced
across every table in the public schema.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Tuple

PUBLIC_CATALOG_TABLES: Tuple[str, ...] = (
    "movies",
    "movie_files",
    "movie_aliases",
    "seasons",
    "episodes",
    "series_seasons",
    "series_episodes",
    "file_seasons",
    "file_episodes",
    "trending_history",
    "trending_meta",
)

USER_DATA_TABLES: Tuple[str, ...] = (
    "miniapp_users",
    "movie_ratings",
    "movie_reviews",
    "user_activity",
    "user_recommendation_events",
    "user_requests",
    "user_upcoming_reminders",
    "user_watchlist",
    "global_chat_messages",
    "global_chat_relay_outbox",
    "upcoming_notifications",
)

INTERNAL_BOT_TABLES: Tuple[str, ...] = (
    "auto_delete_queue",
    "bot_commands",
    "candidate_links",
    "channel_posts",
    "content_identity_repair_archive",
    "ingestion_batch_items",
    "ingestion_batches",
    "ingestion_evidence",
    "pending_leech",
    "schema_migrations",
    "scraped_links",
    "sync_info",
    "t2t_channels",
    "t2t_channels_test",
    "t2t_forward_log",
    "t2t_forward_log_test",
    "temp_links",
)

PROVIDER_CACHE_TABLES: Tuple[str, ...] = (
    "movie_metadata_sync",
    "movie_provider_metadata_cache",
    "movie_provider_ratings_cache",
    "movie_provider_review_sync",
    "movie_provider_reviews_cache",
)

ALL_AUDITED_PUBLIC_TABLES: Tuple[str, ...] = (
    PUBLIC_CATALOG_TABLES
    + USER_DATA_TABLES
    + INTERNAL_BOT_TABLES
    + PROVIDER_CACHE_TABLES
)

_SAFE_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def validate_table_identifier(table_name: str) -> str:
    if not _SAFE_IDENT_RE.match(table_name):
        raise ValueError(f"Unsafe SQL table identifier: {table_name!r}")
    return table_name


def build_rls_remediation_statements(
    tables: Iterable[str] = ALL_AUDITED_PUBLIC_TABLES,
) -> List[str]:
    """
    Generate idempotent PostgreSQL DO blocks that:
      1. Enable Row-Level Security on each existing public table.
      2. Revoke direct anon / authenticated table privileges when those roles exist.
    Safe on both Supabase PostgreSQL and plain local/CI PostgreSQL databases.
    """
    statements: List[str] = []
    for raw_name in tables:
        table = validate_table_identifier(raw_name)
        statements.append(
            f"""
            DO $$
            BEGIN
                IF to_regclass('public.{table}') IS NOT NULL THEN
                    IF EXISTS (
                        SELECT 1 FROM pg_tables
                        WHERE schemaname = 'public' AND tablename = '{table}' AND rowsecurity = false
                    ) THEN
                        EXECUTE 'ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY';
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
                        EXECUTE 'REVOKE ALL ON TABLE public.{table} FROM anon';
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
                        EXECUTE 'REVOKE ALL ON TABLE public.{table} FROM authenticated';
                    END IF;
                END IF;
            END
            $$;
            """.strip()
        )
    return statements
