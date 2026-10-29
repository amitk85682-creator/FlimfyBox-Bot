# Premium movie detail page

## Intent

Make an opened movie feel like a polished cinema title page, help viewers choose a download quality quickly, and make ratings/reviews easy to understand. The user asked for an extraordinary detail view and a combined “Flimfy Verdict” using IMDb and TMDb.

## Goals

- Reorganize the detail hero, metadata, download choices, ratings, and reviews into a clear responsive layout.
- Show IMDb and TMDb scores as separate attributed sources, with vote counts where available.
- Show a Flimfy Verdict equal-weight mean of available IMDb and TMDb scores plus FlimfyBox member ratings on a 0–10 scale; scale the bot's 1–5 average to 0–10, keep each source's vote count separate, and explain when sources agree, diverge, or are missing.
- Load external scores, the FlimfyBox aggregate, and member reviews asynchronously so the existing movie detail request and download flow stay responsive.
- Cover local catalogue cards, upcoming cards, and TMDb-only search/discovery cards without confusing local database IDs with TMDb IDs.
- Show written reviews from FlimfyBox members, stored in the bot's own database; never label or render TMDb reviews as FlimfyBox content.
- Preserve the existing trailer, watchlist, request/availability, season, episode, and per-file download actions.

## Non-goals

- Do not invent or relabel the database `rating` column: its provider is not recorded consistently.
- Keep one editable written review per Telegram member and catalogue title; do not add review replies, reactions, or third-party review imports.
- Do not change download URLs, file identity, search, or other catalogue screens.

## Constraints and behavior

- IMDb and TMDb scores are optional. OMDb supplies the IMDb score only when an IMDb ID and `OMDB_API_KEY` are available; TMDb supplies its score and vote count when a TMDb ID and API key are available. FlimfyBox member ratings come from `movie_ratings`, and written reviews come from `movie_reviews`. Local catalogue IDs and explicit TMDb IDs use distinct route shapes.
- Provider lookups use short timeouts, bounded concurrency, and a short failure cache. The client renders the movie immediately and fills the Verdict, FlimfyBox rating, and member reviews independently.
- The Verdict is the arithmetic mean of available IMDb, TMDb, and FlimfyBox community scores, rounded to one decimal after scaling FlimfyBox's 1–5 score to 0–10. It never uses the unsourced catalogue `rating` column. It reports how many source scores contributed and keeps source vote/rating counts separate. The watch cue is a deterministic label derived from that mean, not a promise or a fabricated audience signal.
- No score renders as “Not enough data”; missing one provider is shown as unavailable rather than as zero.
- Download rows retain the original movie/file IDs and handlers. Best available resolution can be identified from the supplied quality text only.
- Desktop uses a cinematic full-width hero and balanced content columns. On small screens downloads precede the long-form story/reviews. Respect reduced-motion preferences.

## Approaches considered

1. Restyle only the current markup. Lowest risk, but it cannot distinguish IMDb/TMDb or produce a defensible combined score.
2. Put one blended score in the hero. Visually strong, but it hides source availability and can mislead when the inputs are missing.
3. Build a source-aware cinema detail page: separate IMDb/TMDb/FlimfyBox rating sources, transparent Flimfy Verdict, tidy downloads, and first-party member reviews. **Chosen** because it looks distinctive while keeping each score and fallback honest.

## Design calls

- Give the Verdict its own dark, high-contrast card so it stays distinctive in either app theme; use the existing typography, a violet-to-teal accent, and subtle entrance/score transitions. Respect reduced-motion preferences and do not copy another streaming service's page.
- Keep the current user star-rating workflow, add a visible “Rate this” detail action that scrolls to it, and include its public aggregate in the combined Verdict.
- Name the written-review area “From the FlimfyBox community”; show a calm empty state and a signed-in review form when no reviews are returned.
- Build the score presentation as a small reusable frontend module so its calculation and missing-data behavior can be tested independently.

## Acceptance checks

- Opening a detail page never waits for TMDb/OMDb; provider outages produce partial or empty states without breaking downloads.
- Valid IMDb, TMDb, and FlimfyBox scores combine to the documented one-decimal mean; FlimfyBox's 1–5 score is scaled to 0–10, and invalid or absent values are excluded.
- First-party review author/content are rendered as text; unavailable reviews do not show fabricated content, and unauthenticated visitors cannot submit reviews.
- Every download button still calls the existing handler with its original movie and file IDs.
- Desktop and mobile detail layouts remain usable, keyboard-visible, theme-aware, and reduced-motion friendly.
