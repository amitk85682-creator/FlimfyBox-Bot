-- ============================================================================
-- FlimfyBox Supabase Public-Schema RLS Remediation (Migration 21)
-- ============================================================================
-- Purpose:
--   1. Enable Row-Level Security (RLS) on all 44 audited tables in `public`.
--   2. Revoke direct `anon` and `authenticated` PostgREST Data API privileges
--      on every table so no table rows are exposed through Supabase Data API.
--   3. Preserve full backend access for `postgres` / `service_role` used by
--      Render (Python bot + Flask Mini App) and the Node.js companion server.
--
-- Rollback note:
--   Only re-grant specific read privileges on intentionally public tables if
--   a verified browser Supabase client is added in the future.
-- ============================================================================

DO $$
DECLARE
    tbl TEXT;
    audited_tables TEXT[] := ARRAY[
        -- Catalog tables
        'movies', 'movie_files', 'movie_aliases', 'seasons', 'episodes',
        'series_seasons', 'series_episodes', 'file_seasons', 'file_episodes',
        'trending_history', 'trending_meta',
        -- User-owned & community tables
        'miniapp_users', 'movie_ratings', 'movie_reviews', 'user_activity',
        'user_recommendation_events', 'user_requests', 'user_upcoming_reminders',
        'user_watchlist', 'global_chat_messages', 'global_chat_relay_outbox',
        'upcoming_notifications',
        -- Internal bot & ingestion tables
        'auto_delete_queue', 'bot_commands', 'candidate_links', 'channel_posts',
        'content_identity_repair_archive', 'ingestion_batch_items',
        'ingestion_batches', 'ingestion_evidence', 'pending_leech',
        'schema_migrations', 'scraped_links', 'sync_info', 't2t_channels',
        't2t_channels_test', 't2t_forward_log', 't2t_forward_log_test',
        'temp_links',
        -- Provider cache & sync tables
        'movie_metadata_sync', 'movie_provider_metadata_cache',
        'movie_provider_ratings_cache', 'movie_provider_review_sync',
        'movie_provider_reviews_cache'
    ];
BEGIN
    FOREACH tbl IN ARRAY audited_tables LOOP
        IF to_regclass(format('public.%I', tbl)) IS NOT NULL THEN
            EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', tbl);
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
                EXECUTE format('REVOKE ALL ON TABLE public.%I FROM anon', tbl);
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
                EXECUTE format('REVOKE ALL ON TABLE public.%I FROM authenticated', tbl);
            END IF;
        END IF;
    END LOOP;
END
$$;

-- ============================================================================
-- Post-Migration Verification Queries (run after applying above)
-- ============================================================================
-- 1. Must return 0 rows (no public tables left with RLS disabled):
--    SELECT tablename FROM pg_tables
--    WHERE schemaname = 'public' AND rowsecurity = false
--    ORDER BY tablename;
--
-- 2. Must return 0 rows (no direct anon/authenticated table grants remaining):
--    SELECT table_name, grantee, privilege_type
--    FROM information_schema.role_table_grants
--    WHERE table_schema = 'public' AND grantee IN ('anon', 'authenticated')
--    ORDER BY table_name, grantee;
