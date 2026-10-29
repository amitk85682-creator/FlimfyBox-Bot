# Advanced Bot Reliability Implementation Plan

> **For agentic workers:** Execute the three tasks in order. Each task begins with a regression test and runs its relevant verification before moving on.

**Goal:** Improve batch isolation, user-facing responsiveness, Upcoming reliability, and mini-app interaction correctness without replacing the current stack.

**Architecture:** Preserve Python/Flask and existing APIs. Keep the identity timeout separate from per-file saves, bound external calls, and move synchronous database work out of hot async handlers.

**Tech Stack:** Python 3.12, python-telegram-bot, Flask, PostgreSQL/psycopg2, browser JavaScript, Node test runner.

**Spec:** `docs/superpowers/specs/2026-10-29-advanced-bot-reliability-design.md`

## Global Constraints

- Five minutes applies only to movie identity/metadata resolution; do not add a whole-save timeout.
- One item error must not abort later files or hide valid partial Upcoming data.
- Preserve current API fields and existing theme/search behavior.
- Do not access production Supabase, Render, or Telegram services during local verification.

## Review Focus

- A slow save must not be marked failed while its worker continues side effects; keep operation-level timeouts and checkpoint semantics.
- One TMDB discovery source failing must not be reported as a confirmed empty result; both sources failing must remain retryable.
- An older Upcoming response must not overwrite a newer home render.
- Generated controls must remain keyboard-operable and image failure must produce a usable placeholder.

---

### Task 1: SuperBatch save boundary

**Files:** `main.py`, `tests/test_superbatch_timeouts.py`

**Interfaces:** Keep `_save_superbatch_files(movie_files, context, movie_state, progress=None)` unchanged. Keep the five-minute identification limit in `_superbatch_done_impl`.

- [x] Add a regression asserting file saving is not wrapped in `SUPERBATCH_FILE_TIMEOUT_SECONDS`, while a per-file exception still checkpoints failure and later files continue.
- [x] Run the focused regression and confirm it fails against the current 180-second aggregate timeout.
- [x] Remove the aggregate save timeout and its obsolete constant; retain individual Telegram/database timeouts and per-file exception isolation.
- [x] Run focused batch verification and Python syntax compilation.

### Task 2: Search responsiveness and Upcoming resilience

**Files:** `main.py`, `webapp_routes.py`, `static/miniapp/app.js`, `tests/test_search_latency.py`, `tests/test_miniapp_autocomplete_dates.py`, `tests/miniapp_search_theme.test.js`

**Interfaces:** Keep `/api/upcoming` response `status` and `movies`; optional `partial` indicates a single-source provider failure. Keep current search endpoint and cached prefix behavior.

- [x] Add regression coverage for a hot async handler using executor-backed DB work, partial/total TMDB failures, per-request timeout budget, stale upcoming response, and no synchronous SQL on the event loop.
- [x] Run each focused regression and confirm it exposes the current behavior.
- [x] Offload My Stats and the active batch's remaining direct database reads/writes; keep interactive searches on their dedicated executor.
- [x] Bound Upcoming provider attempts and use stale successful TMDB data on transient errors; return retryable failure only if both sources have no usable data.
- [x] Ignore late Upcoming responses after a newer home render starts.
- [x] Run focused checks and compile modified Python modules.

### Task 3: Mini App control correctness

**Files:** `templates/mini_app.html`, `static/miniapp/app.js`, `static/miniapp/cinema.css`, `tests/miniapp_interactions.test.js`

**Interfaces:** Keep existing detail, genre, and season functions; use native buttons and existing action functions.

- [x] Add regression coverage for the genre-detail back control, genre search accessible name, season buttons and `aria-pressed`, modal close names, and poster fallback.
- [x] Run the new Node checks and confirm they fail against the current markup/handlers.
- [x] Wire the genre-detail control to return to genres, render season controls as buttons, add accessible labels, and standardize image failure fallback.
- [x] Run the full Node mini-app checks and Python syntax compilation.

## Verification

- [x] Run all Node tests under `tests/*.test.js` and the trailer playback test.
- [x] Compile all modified Python files with `python -m py_compile`.
- [ ] Run the full pytest suite when the project dependency environment is available (the bundled runtime lacks pytest, Flask, requests, python-telegram-bot, and psycopg2).
- [x] Review `git diff --check` and inspect the final diff before any push.
