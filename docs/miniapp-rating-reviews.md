# Mini App rating and review fetching

## Provider APIs

The browser calls FlimfyBox backend endpoints only. Provider credentials are read by the Python backend and never sent to browser JavaScript. Rating responses include per-source states (`fresh`, `stale`, `stale_cached`, `refreshing`, `unavailable`, `not_configured`, or `unlinked`) so a missing title link is not presented as an upstream outage. These states describe data availability/cache behavior; they are not the provider's raw HTTP status code.

- **TMDb ratings:** `GET /3/{movie|tv}/{tmdb_id}?append_to_response=external_ids`. This supplies TMDb audience score, vote count, and (when available) the IMDb ID.
- **IMDb rating:** OMDb `GET https://www.omdbapi.com/?i={imdb_id}`. FlimfyBox reads `imdbRating` and `imdbVotes`; this API is used because it exposes IMDb scores without scraping IMDb pages.
- **TMDb written reviews:** `GET /3/{movie|tv}/{tmdb_id}/reviews?language=en-US&page=1`. Up to five valid review excerpts are included.
- **FlimfyBox member ratings and reviews:** read from the FlimfyBox database on each detail open so member content and the signed-in member's own rating stay current. They are not part of the provider cache.

For a catalogue title, the browser requests `/api/movie/{id}/rating-sources`, `/api/movie/{id}/rating`, `/api/movie/{id}/reviews`, and `/api/movie/{id}/audience-reviews` as separate operations. For a TMDb-only title, it requests the `/api/tmdb/{type}/{id}/...` provider endpoints. The browser merges available FlimfyBox and TMDb reviews; one failed request does not discard the successful source. The unified meter combines valid IMDb and TMDb scores with the FlimfyBox member average when present.

## Cache policy

Successful provider responses are cached in backend process memory. Cache TTLs are configurable in seconds and clamped to 1 second–30 days.

| Cache | Environment variable | Default |
| --- | --- | ---: |
| TMDb title/rating details | `MINIAPP_TMDB_RATING_CACHE_SECONDS` | 6 hours |
| OMDb IMDb score | `MINIAPP_OMDB_RATING_CACHE_SECONDS` | 6 hours |
| TMDb reviews | `MINIAPP_TMDB_REVIEW_CACHE_SECONDS` | 6 hours |
| Successful combined provider rating response | `MINIAPP_RATING_RESPONSE_CACHE_SECONDS` | 5 minutes |
| Provider failure cooldown | `MINIAPP_PROVIDER_FAILURE_CACHE_SECONDS` | 60 seconds |
| Stale provider retry cooldown | `MINIAPP_PROVIDER_STALE_RETRY_SECONDS` | 30 seconds |

Valid provider cache entries are returned without calling that provider again. Missing entries are fetched once per key within the process; concurrent callers join the same in-flight request. A still-usable stale value is returned immediately while one background refresh runs. If a refresh fails, the last good provider value remains cached for the stale retry cooldown. A provider error, malformed response, or rate limit cannot replace good data with an empty payload. Successful empty TMDb review results are treated as valid provider results. In addition to the configurable response TTL above, incomplete rating responses are rechecked after 1 second, while provider failures use the configured 60-second default cooldown and stale data uses the configured 30-second retry cooldown.

The combined rating cache lasts five minutes by default; the independent six-hour provider caches still decide when TMDb/OMDb are called. This avoids rebuilding the same aggregate on every detail open without delaying FlimfyBox member-rating updates, which use a separate uncached endpoint. Failure and stale cooldowns prevent request storms during outages. TMDb 429 responses are not immediately retried; transient server errors get at most one retry for rating/detail requests. OMDb and review requests are not retried immediately.

The current caches and single-flight locks are **process-local**, not shared across multiple Render instances or restarts. With more than one server process, each process can make one request for the same expired key; a shared cache/lock would be needed to deduplicate across instances.

## Page behavior and outage handling

The browser stores valid provider rating data and public review lists in versioned `localStorage` entries. Ratings are considered fresh for five minutes, reviews for one minute, and stale entries are retained for up to seven days. Cached entries render synchronously before network requests; expired entries remain visible during revalidation. The cache excludes user-specific review fields and rating credentials. A provider failure or empty rating response does not replace a saved valid rating. This browser cache survives Render restarts for that user/device; backend caches remain process-local.

Opening a detail page starts rating, member-rating, and review requests independently after the detail shell is visible. Those requests do not gate the title, downloads, trailer, or other detail actions. The rating-source request uses a 4.5-second frontend timeout, member-rating and FlimfyBox review requests 3.5 seconds, TMDb reviews 4 seconds, and the movie-detail request 7 seconds; unrelated requests retain the existing 12-second default. The rating card shows a restrained loading state only when no cached score exists. Stale ratings get at most two background rechecks with exponential delays and jitter. FlimfyBox and TMDb reviews render independently as each request completes; a slow or failed feed does not hold back the other. Stale TMDb reviews get at most two background rechecks. Moving to another title aborts obsolete browser requests, and request IDs prevent late responses from overwriting the active title.

Rating/detail provider requests use bounded connect/read timeouts and a bounded worker pool. Known IMDb and TMDb identities are fetched concurrently; OMDb is requested after TMDb only when TMDb must first supply the external IMDb ID. Provider request/cache events and detail endpoint durations are logged with status and elapsed milliseconds, without URLs, query parameters, or credentials. Provider failures are logged by type; each actual provider request has one timing log, so calls can be counted per cache key/title.

If TMDb or OMDb fails, the other source can still update. Previously cached scores/reviews stay visible; a source with no saved data is omitted, and the interface explains when ratings or reviews are temporarily unavailable. A title with no linked rating identity is labeled as unlinked rather than as a provider outage. FlimfyBox member content still loads independently from the database. API failure details and credentials are not returned to the browser or written to logs; logs record cache hit/miss, provider failure type, and rate limiting without logging request URLs or keys.

Review totals are exact only when every expected feed succeeded and the title is linked to TMDb. If a feed failed or is unlinked, the UI shows “Count unavailable” for an empty result or “At least N reviews” when another feed returned reviews. A successful empty response from both linked feeds is shown as “0 reviews.”

IMDb written reviews are not available through the OMDb rating response and are not scraped. Review excerpts in the merged feed are FlimfyBox member reviews and public TMDb reviews, with imported entries retaining clear source attribution.
