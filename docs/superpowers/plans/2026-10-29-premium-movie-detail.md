# Premium movie detail implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the movie detail page with source-aware IMDb/TMDb ratings, a transparent Moctale Meter, a tidy download chooser, and sourced review excerpts.

**Architecture:** Add a bounded, cached public endpoint for provider scores and TMDb reviews; keep its request asynchronous from the existing detail response. Add a small pure frontend rating module and use it in the existing detail page while retaining current actions and APIs.

**Tech Stack:** Flask/Python, PostgreSQL, vanilla JavaScript, CSS, pytest, Node's built-in test runner.

**Spec:** `docs/superpowers/specs/2026-10-29-premium-movie-detail-design.md`

## Global Constraints

- Never label the database `rating` value as IMDb or TMDb.
- Blend only valid source scores on a 0–10 scale and identify the contributing providers.
- Provider data is optional and must never block the existing movie detail/download response.
- Local movie IDs and TMDb movie/TV IDs use distinct route parameters; never infer one ID namespace from another.
- Preserve the existing movie/file IDs and actions for every download row.
- Use text nodes for provider review content and allow links only to TMDb review URLs.

## Review Focus

- One provider is missing or times out: return usable partial scores and keep the page interactive; test in Task 1.
- Both sources are absent, malformed, or outside 0–10: show no fabricated score; test in Task 2.
- Review content contains HTML and a link points off TMDb: render it safely and omit unsafe links; test in Task 3.
- Detail is opened rapidly for two different movies: stale scores/reviews cannot replace the current title; test in Task 3.
- A title has multiple files or episode qualities: each button retains its own IDs and current download handler; test in Task 3.

### Task 1: Provider score and review endpoint

**Files:**
- Modify: `webapp_routes.py`
- Test: `tests/test_miniapp_rating_sources.py`

**Interfaces:**
- Produces `GET /api/movie/<int:movie_id>/rating-sources` for local cards and `GET /api/tmdb/<media_type>/<int:tmdb_id>/rating-sources` for TMDb-only/upcoming cards, both returning `status`, `imdb`, `tmdb`, `reviews`, and `review_count`.
- `imdb` contains `score` and `votes`; `tmdb` contains `score` and integer `votes`; each may be null when unavailable.
- Each review contains `author`, `content`, `rating`, `created_at`, and a provider URL.

- [ ] Write route tests for both provider scores + reviews, partial provider failure, caching, missing local IDs, and TMDb-only movie/TV IDs.
- [ ] Run those tests and confirm they fail because the route is absent.
- [ ] Implement the route with parallel provider calls, short network deadlines, and bounded cache TTLs.
- [ ] Run the route tests and the existing webapp route/auth tests.

### Task 2: Moctale Meter calculation

**Files:**
- Create: `static/miniapp/rating-meter.js`
- Modify: `templates/mini_app.html`
- Create: `tests/miniapp_rating_meter.test.js`

**Interfaces:**
- `window.MoctaleMeter.summarize({ imdb, tmdb })` returns `score`, `sourceCount`, and contributing source names.
- `window.MoctaleMeter.render(container, payload)` fills the meter, source cards, and review list without interpreting arbitrary HTML.

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
- `communityRating` remains the existing FlimfyBox rating UI; provider ratings and TMDb review excerpts are rendered in the new Meter/review panels.

- [ ] Add behavior tests for independent score loading, stale-response protection, and download-file ID preservation.
- [ ] Run the relevant Node tests and confirm the new interaction contracts fail on missing layout/logic.
- [ ] Implement the responsive hero/content columns, download cards, provider Meter, sourced review cards, empty/error states, and reduced-motion handling.
- [ ] Run frontend tests and inspect the local preview on desktop and mobile widths.

### Task 4: Verification and integration

**Files:**
- Review all files changed above.

- [ ] Run all available Python route tests and all Node mini-app tests; record any environment-limited commands.
- [ ] Run Python syntax compilation and `git diff --check`.
- [ ] Verify the local preview has no console/runtime errors and each download action remains wired.
- [ ] Commit the finished change on the user's requested `main` branch after verification.
