# Mini App rating and review fetching

## Provider APIs

The browser calls FlimfyBox backend endpoints only. Provider credentials are read by the Python backend and never sent to browser JavaScript.

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
| Successful combined provider rating response | `MINIAPP_RATING_RESPONSE_CACHE_SECONDS` | 15 seconds |
| Provider failure cooldown | `MINIAPP_PROVIDER_FAILURE_CACHE_SECONDS` | 60 seconds |
| Stale provider retry cooldown | `MINIAPP_PROVIDER_STALE_RETRY_SECONDS` | 30 seconds |

Valid provider cache entries are returned without calling that provider again. Missing entries are fetched once per key within the process; concurrent callers join the same in-flight request. A still-usable stale value is returned immediately while one background refresh runs. If a refresh fails, the last good provider value remains cached for the stale retry cooldown. A provider error, malformed response, or rate limit cannot replace good data with an empty payload. Successful empty TMDb review results are treated as valid provider results. In addition to the configurable response TTL above, incomplete rating responses are rechecked after 1 second, while provider failures use the configured 60-second default cooldown and stale data uses the configured 30-second retry cooldown.

The response-level rating cache is deliberately short so combined values reflect provider updates, while the source caches prevent repeated provider requests on page renders. Failure and stale cooldowns prevent request storms during outages. TMDb 429 responses are not immediately retried; transient server errors get at most one retry for rating/detail requests. OMDb and review requests are not retried immediately.

The current caches and single-flight locks are **process-local**, not shared across multiple Render instances or restarts. With more than one server process, each process can make one request for the same expired key; a shared cache/lock would be needed to deduplicate across instances.

## Page behavior and outage handling

Opening a detail page starts rating, member-rating, and review requests independently after the detail shell is visible. Those requests do not gate the title, downloads, trailer, or other detail actions. The rating card shows a restrained loading state. A stale rating remains visible with a background-refresh message; the open detail page quietly checks up to four times for the refreshed value. Stale reviews remain in the feed with a small update/saved label while a similar bounded check picks up a successful refresh. Moving to another title aborts obsolete browser requests.

If TMDb or OMDb fails, the other source can still update. Previously cached scores/reviews stay visible; a source with no saved data is omitted, and the interface explains when ratings or reviews are temporarily unavailable. FlimfyBox member content still loads independently from the database. API failure details and credentials are not returned to the browser or written to logs; logs record cache hit/miss, provider failure type, and rate limiting without logging request URLs or keys.

IMDb written reviews are not available through the OMDb rating response and are not scraped. Review excerpts in the merged feed are FlimfyBox member reviews and public TMDb reviews, with imported entries retaining clear source attribution.
