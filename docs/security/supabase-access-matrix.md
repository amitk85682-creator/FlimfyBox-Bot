# Supabase public-schema access review — Phase 0

Reviewed: 2026-11-09
Project: `zynzkjbwhcoppvykcqrc`
Scope: read-only inventory. No migration was applied.

## Findings

The `public` schema contains 44 tables and no public views or materialized views. The Data API security advisor reported 36 tables with RLS disabled and 8 with RLS enabled but no policies. The database ACL check showed `anon` and `authenticated` had every table privilege on all 44 tables. With RLS disabled, those grants make the table rows available to those roles through an exposed Data API. RLS-enabled tables with no policies deny row access to those roles, despite their grants.

Supabase's current [API security guide](https://supabase.com/docs/guides/api/securing-your-api) confirms that table grants and RLS work together: grants decide whether a role can reach an object; RLS decides which rows it can read or change.

### RLS was disabled

`auto_delete_queue`, `channel_posts`, `content_identity_repair_archive`, `episodes`, `file_episodes`, `file_seasons`, `global_chat_messages`, `global_chat_relay_outbox`, `ingestion_batch_items`, `ingestion_batches`, `ingestion_evidence`, `miniapp_users`, `movie_aliases`, `movie_files`, `movie_ratings`, `movie_reviews`, `pending_leech`, `schema_migrations`, `scraped_links`, `seasons`, `series_episodes`, `series_seasons`, `sync_info`, `t2t_channels`, `t2t_channels_test`, `t2t_forward_log`, `t2t_forward_log_test`, `temp_links`, `trending_history`, `trending_meta`, `upcoming_notifications`, `user_activity`, `user_recommendation_events`, `user_requests`, `user_upcoming_reminders`, `user_watchlist`.

### RLS was enabled, with no policies

`bot_commands`, `candidate_links`, `movie_metadata_sync`, `movie_provider_metadata_cache`, `movie_provider_ratings_cache`, `movie_provider_review_sync`, `movie_provider_reviews_cache`, `movies`.

## Access-path evidence

- The Mini App routes call the Flask backend; no Supabase browser client, Supabase REST URL, or `supabase-js` usage was found in the repository search.
- The bot and Flask server use `DATABASE_URL` for PostgreSQL connections.
- The queried Supavisor logs for the last 24 hours showed 22,590 connections with database role `postgres` and no other displayed database role. PostgREST logs had one entry in that window. This supports a backend-only access model, but does not prove that no external client exists.
- The Supabase MCP SQL session itself reports role `postgres` with `BYPASSRLS`; it is not direct proof of Render's environment. The Supavisor evidence is not enough to prove which database role the Render service uses or whether external consumers exist.

## Security rollout decision

No production schema or privilege change was applied in Phase 0. The proposed broad change—enabling RLS and removing `anon` / `authenticated` grants across the entire public schema—was rejected by the automatic approval review because it could disable unknown external Data API clients and the Render service's live database role has not been directly verified. No equivalent migration is present in the application code.

Before that change can be reviewed again, confirm any external Data API consumers and verify the exact role used by Render. Then split the rollout by table: public catalogue reads, user-owned data, internal bot state, and provider caches need separate policies and grants. Preserve the current service connection path and validate Mini App reads, user writes, bot ingestion, ratings, reviews, and global chat against a staging database before production.

After an approved table-specific rollout, verify that each table has RLS, that only intended roles retain grants, that `schema_migrations` records the migration, and that Render starts with the Mini App and bot functioning. Re-run Supabase security advisors and confirm the `rls_disabled_in_public` findings are resolved.
