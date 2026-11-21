# FlimfyBox Architecture, Security & Deployment Operations

## 1. Single Authoritative Production Architecture

- **Production Authority (Render `render.yaml`)**:
  - `main.py` + `webapp_routes.py` (Python 3.12 + Flask/Waitress + `python-telegram-bot`) is the single authoritative production runtime handling Telegram bot updates, `/start tmp_...` file delivery, `/batch`, `/superbatch`, `/batch18`, background sync jobs, and the Mini App HTTP routes.
  - **Companion / Preview Backend (`server.ts`)**: Express + TypeScript runtime serving the exact same Mini App frontend (`templates/mini_app.html`, `static/miniapp/*`), sharing the **exact same Supabase PostgreSQL schema** (`movies`, `movie_files`, `temp_links`, `global_chat_messages`, `miniapp_users`) via `src/db.ts`.
- **Unified Download Token Flow (`temp_links`)**:
  - Both Python (`webapp_routes.py`) and Node (`server.ts` -> `src/db.ts`) write short-lived `tmp_<hex>` tokens into PostgreSQL `temp_links(token, movie_id, movie_file_id, created_at)`.
  - Clicking "Open in Telegram" launches `https://t.me/<BOT_USERNAME>?start=tmp_<hex>`, which Python `main.py` resolves directly from PostgreSQL `temp_links` and delivers the actual media files.
- **Data Mode Separation (`APP_DATA_MODE`)**:
  - `APP_DATA_MODE=production` (default whenever `DATABASE_URL` / `DATABASE_POOLER_URL` is configured and reachable): serves live PostgreSQL data only.
  - `APP_DATA_MODE=demo` (or when `DATABASE_URL` is unset in local preview/CI): serves deterministic demo fixtures (`src/catalog.ts`, `src/store.ts`) with zero writes to production tables.

## 2. Main Branch Protection Checklist (GitHub Settings -> Branches)

Configure a branch protection rule for `main`:
1. Enable **Require a pull request before merging** (minimum 1 approval for team repos).
2. Enable **Require status checks to pass before merging** and select all 3 CI jobs from `.github/workflows/ci.yml`:
   - `Node.js Build, Lint & Tests`
   - `Python Compile & Unit Tests`
   - `Secret & Config Hygiene Check`
3. Enable **Require branches to be up to date before merging**.
4. Disable force pushes (`Allow force pushes`: off) and branch deletions (`Allow deletions`: off).

## 3. Supabase RLS Remediation & Verification

- Migration `21` in `db_migrations.py` (backed by `services/rls_audit.py` and mirrored in `docs/security/rls-remediation.sql`) enables Row-Level Security on all 44 `public` tables and revokes `anon` and `authenticated` table privileges while retaining full access for the backend `postgres` / `service_role`.
- Verify after deployment in Supabase SQL Editor:
  - `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND rowsecurity = false;` -> `0 rows`
  - `SELECT table_name, grantee FROM information_schema.role_table_grants WHERE table_schema = 'public' AND grantee IN ('anon', 'authenticated');` -> `0 rows`

## 4. Backup & Rollback Playbook

### Application Rollback (Render)
1. Open Render Dashboard -> `flimfybox-bot` -> **Events / Deploys**.
2. Click **Rollback to this deploy** on the last known green commit (instant container switch, <60s).
3. In Git, revert cleanly via `git revert <bad_commit_sha>` and push through CI rather than force-pushing `main`.

### Database Backup & Migration Rollback (Supabase PostgreSQL)
1. **Before running schema migrations**: take a logical snapshot:
   ```bash
   pg_dump "$DATABASE_URL" --format=custom --no-owner --no-privileges -f "backup_$(date +%Y%m%d_%H%M%S).dump"
   ```
2. **Automated Backups**: Enable Supabase Daily Backups / Point-in-Time Recovery (PITR) on the production project.
3. **Migration Hygiene**: All migrations in `db_migrations.py` are additive/idempotent (`IF NOT EXISTS`, `to_regclass` guards) and recorded in `schema_migrations`, so rolling back app code to the previous commit remains schema-compatible without destructive down-migrations.
4. **Emergency Restore**:
   ```bash
   pg_restore --clean --if-exists --no-owner --dbname="$DATABASE_URL" backup_YYYYMMDD_HHMMSS.dump
   ```

## 5. Health & Monitoring Endpoints

- `GET /healthz`: Liveness + active `data_mode`, DB connectivity state, and uptime.
- `GET /readyz`: Returns `200` when ready (`503` if `APP_DATA_MODE=production` and PostgreSQL is unreachable).
- Structured JSON request logs include `X-Request-Id`, HTTP status, duration, and automatic redaction of tokens/credentials.
