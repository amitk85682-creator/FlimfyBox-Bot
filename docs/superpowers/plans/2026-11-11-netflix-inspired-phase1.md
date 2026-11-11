# Netflix-Inspired Phase 1 Implementation Plan

> **For agentic workers:** Use the native inline execution method. Work one task at a time, with a regression test before implementation and verification after it.

**Goal:** Make short Telegram bot searches use the indexed prefix path and return without expensive fallbacks when nothing starts with that prefix.

**Architecture:** Keep the existing search handler, PostgreSQL pool, and Mini App API. For one- and two-character normalized bot queries, use the existing expression B-tree prefix index; retain the existing contains/trigram path for longer searches.

**Tech Stack:** Python, PostgreSQL `text_pattern_ops` expression index, pytest, python-telegram-bot handler.

**Spec:** `docs/superpowers/specs/2026-11-11-netflix-inspired-reliability-design.md`

## Global Constraints

- Do not change search result message formats, Mini App APIs, file delivery, or long-query fuzzy behavior.
- Do not add dependencies or database migrations; the normalized prefix index already exists in production.
- Do not make external Google/provider calls for one- or two-character misses.
- Do not include search text, user identifiers, or secrets in new timing logs.

## Review Focus

- One-character and two-character inputs must use a trailing wildcard only and stop before fuzzy scans.
- A short-prefix miss must not fall through to alias scans or Google suggestions.
- Punctuation normalization must preserve matches such as `re` → `Reacher`.
- Queries with three or more normalized characters must retain the current contains and trigram fallbacks.
- A database error must preserve the current user-visible failure behavior rather than report a confident match.

---

### Task 1: Telegram short-prefix lookup

**Files:**
- Modify: `main.py` (`_get_movies_fast_sql_nocache`, `search_movies`)
- Test: `tests/test_search_latency.py`

**Interfaces:**
- Keep `_get_movies_fast_sql_nocache(query: str, limit: int = 5)` unchanged.
- Keep `search_movies(update, context, query_text=None)` response messages and downstream result handling unchanged.
- Reuse `_normalize_search_text` and the existing `idx_movies_title_norm_prefix` index.

- [x] Add a parameterized regression for normalized one- and two-character queries. Assert the first SQL pattern is `<prefix>%` and that no trigram query runs.
- [x] Add a short-prefix miss regression. Assert the handler does not call `get_movies_from_db` or Google suggestions.
- [x] Run both new tests and verify they fail against the current contains-first implementation.
- [x] Add a prefix-only SQL branch for normalized query lengths below three; keep the current contains and trigram branches for longer queries.
- [x] Skip the legacy fallback and external suggestions for short-prefix misses.
- [x] Run the targeted search tests, focused Python suite, all JavaScript tests, Python compilation, and `git diff --check`.
- [x] Review the diff for message/API changes and secret exposure.
- [x] Commit on the feature worktree (`781de92`).
- [x] Push the authorized change to `main` (`781de92`).
- [ ] Deploy the configured Render service and verify the returned `/healthz` revision before reporting completion.

## Phase Gates

- Phase 1 Task 1 is code-complete only when the red/green regression is demonstrated and the tests above pass.
- Further cache, query-index, process-isolation, or security work requires its own measured task from the Phase 1–5 roadmap; do not bundle it into this change.

---

### Task 2: Telegram multiple-result disambiguation

**Files:**
- Modify: `main.py` (`_select_single_search_result`, fast and fallback search queries, `create_movie_selection_keyboard`, `search_movies`, `background_search_and_send`, `handle_group_message`, retry-search callback)
- Test: `tests/test_search_latency.py`

- [x] Add regressions proving an exact Spider-Man title cannot bypass related matches, Telegram fetches the bounded result set, and result buttons show year/type/file availability with working pagination.
- [x] Change direct opening to only a single unique catalogue result; otherwise retain the full set for user selection across PM, start-link, retry-suggestion, and group searches.
- [x] Raise Telegram's result window to 100 and include the media type and file-availability fields in fast and fallback search results.
- [x] Keep the existing five-result pages and add concise, Telegram-safe labels for year, media type, and file availability.
- [x] Verify the updated search SQL read-only against Supabase. The normalized Spider-Man query returned 17 records and used `idx_movies_title_norm_trgm` plus the movie-file index; one run measured 1.839 ms execution (4.506 ms planning), with a prior 51 ms outlier.
- [x] Run focused search regressions, Python compilation, and `git diff --check`; document that the complete search-test file stalls in an unrelated Windows async queue test.
- [x] Review the full diff for unrelated changes, message/API regressions, destructive operations, and secrets.
- [x] Commit the chooser fix (`58868c8`).
- [x] Push the chooser fix to `main` (`58868c8`).
- [ ] Deploy/verify Render.

**Bound:** The Telegram chooser holds at most 100 matches per search in user state. If a broader term reaches that cap, the user should refine the query; the UI must say it is showing the first 100 rather than imply it found every match.
