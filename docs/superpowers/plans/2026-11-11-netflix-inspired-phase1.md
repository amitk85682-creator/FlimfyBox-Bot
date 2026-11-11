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
- [ ] Commit on the feature worktree.
- [ ] Push the authorized change to `main`.
- [ ] Deploy the configured Render service and verify the returned `/healthz` revision before reporting completion.

## Phase Gates

- Phase 1 Task 1 is code-complete only when the red/green regression is demonstrated and the tests above pass.
- Further cache, query-index, process-isolation, or security work requires its own measured task from the Phase 1–5 roadmap; do not bundle it into this change.
