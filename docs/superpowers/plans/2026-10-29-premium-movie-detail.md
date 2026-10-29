# Premium movie detail implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the movie detail page with source-aware IMDb/TMDb/FlimfyBox ratings, a transparent Flimfy Verdict, a tidy download chooser, and first-party member reviews.

**Architecture:** Keep provider scores, FlimfyBox community rating, and first-party member reviews as separate asynchronous reads; the combined Verdict normalizes the bot's 5-star mean to the 10-point sources. Add a versioned review table and authenticated, editable review submission while retaining the existing star rating and file actions.

**Tech Stack:** Flask/Python, PostgreSQL, vanilla JavaScript, CSS, pytest, Node's built-in test runner.

**Spec:** `docs/superpowers/specs/2026-10-29-premium-movie-detail-design.md`

## Global Constraints

- Never label the database `rating` value as IMDb or TMDb.
- Blend only valid source scores on a 0–10 scale and identify the contributing providers. Make the title cue deterministic and explain real provider agreement/disagreement; never imply votes or popularity we do not have.
- Provider data is optional and must never block the existing movie detail/download response.
- TMDb/IMDb supply ratings only; member-written content is read from and written to FlimfyBox's own review table.
- Local movie IDs and TMDb movie/TV IDs use distinct route parameters; never infer one ID namespace from another.
- Preserve the existing movie/file IDs and actions for every download row.
- Render member review text as text nodes; never interpret review content as HTML.

## Review Focus

- One provider is missing or times out: return usable partial scores and keep the page interactive; test in Task 1.
- Both sources are absent, malformed, or outside 0–10: show no fabricated score; test in Task 2.
- Review content contains HTML-like text or exceeds the allowed length: render it as plain text and reject overlong submissions; test in Task 1 and Task 2.
- Detail is opened rapidly for two different movies: stale scores/reviews cannot replace the current title; test in Task 3.
- A title has multiple files or episode qualities: each button retains its own IDs and current download handler; test in Task 3.

### Task 1: Provider score endpoints and first-party review API

**Files:**
- Modify: `webapp_routes.py`
- Modify: `db_migrations.py`
- Test: `tests/test_miniapp_rating_sources.py`, `tests/test_miniapp_community_reviews.py`, `tests/test_migrations.py`

**Interfaces:**
- Produces provider score routes returning `status`, `imdb`, and `tmdb`; it does not request third-party review text.
- Produces `GET` and `POST /api/movie/<int:movie_id>/reviews`, with public review reads and one Telegram-authenticated, editable 20–800 character review per member/title.
- `imdb` contains `score` and `votes`; `tmdb` contains `score` and integer `votes`; each may be null when unavailable.
- First-party review reads return `author`, plain-text `content`, optional member `rating`, and `created_at`; there is no third-party review URL.

- [x] Write route tests for both provider scores, partial provider failure, caching, missing local IDs, and TMDb-only movie/TV IDs; separately cover signed member review reads and writes.
- [x] Implement the provider route with parallel provider calls, short network deadlines, and bounded cache TTLs; add the first-party review table and authenticated review API.
- [x] Run the route tests and the existing webapp route/auth tests.

### Task 2: Flimfy Verdict calculation

**Files:**
- Create: `static/miniapp/rating-meter.js`
- Modify: `templates/mini_app.html`
- Create: `tests/miniapp_rating_meter.test.js`

**Interfaces:**
- `window.FlimfyVerdict.summarize({ imdb, tmdb, flimfy })` returns `score`, `sourceCount`, and contributing source names; `verdictForSummary` adds a short watch cue and `audienceRead` explains source agreement or disagreement.
- `window.FlimfyVerdict.render(container, payload)` fills the meter and source cards; `renderReviews` safely renders bot-owned member reviews without interpreting arbitrary HTML.

- [ ] Add tests for two-source mean, single-source fallback, and malformed/missing input.
- [ ] Run the new Node tests and confirm the missing module fails.
- [ ] Implement the pure summary function and safe rendering helpers.
- [ ] Run the new Node tests.

### Task 3: Detail layout and resilient loading

**Files:**
- Modify: `templates/mini_app.html`
- Modify: `static/miniapp/app.js`
- Modify: `static/miniapp/cinema.css`
- Modify: `tests/miniapp_interactions.test.js` and `tests/frontend_reliability.test.js`

**Interfaces:**
- Detail open starts `/api/movie/<id>/rating-sources` independently of `/api/movie/<id>` and guards it with the active detail request ID.
- TMDb-only cards call `/api/tmdb/<media_type>/<tmdb_id>/rating-sources`; their numeric IDs are never sent to the local movie route.
- `dpLinks` presents sorted, clearly labeled file choices but continues to call `downloadMovie(movieId, fileId)`.
- `communityRating` remains the existing FlimfyBox star-rating UI and has a “Rate this” shortcut; its average joins IMDb and TMDb in the Verdict, while first-party written reviews load in their own panel.

- [ ] Add behavior tests for independent score loading, stale-response protection, and download-file ID preservation.
- [ ] Run the relevant Node tests and confirm the new interaction contracts fail on missing layout/logic.
- [ ] Implement the responsive hero/content columns, download cards, three-source Verdict, first-party review cards, empty/error states, and reduced-motion handling.
- [ ] Run frontend tests and inspect the local preview on desktop and mobile widths.

### Task 4: Verification and integration

**Files:**
- Review all files changed above.

- [ ] Run all available Python route tests and all Node mini-app tests; record any environment-limited commands.
- [ ] Run Python syntax compilation and `git diff --check`.
- [ ] Verify the local preview has no console/runtime errors and each download action remains wired.
- [ ] Commit the finished change on the user's requested `main` branch after verification.
