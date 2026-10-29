# Premium movie detail page

## Intent

Make an opened movie feel like a polished cinema title page, help viewers choose a download quality quickly, and make ratings/reviews easy to understand. The user asked for an extraordinary detail view and a combined “Flimfy Verdict” using IMDb and TMDb.

## Goals

- Reorganize the detail hero, metadata, download choices, ratings, and reviews into a clear responsive layout.
- Show IMDb and TMDb scores as separate attributed sources, with vote counts where available.
- Show a Flimfy Verdict equal-weight mean of the valid available IMDb and TMDb scores on a 0–10 scale; pair it with a plain-language watch cue, separate provider scores/votes, and an honest note when sources agree, diverge, or one is missing.
- Load external score/review data asynchronously and cache it so the existing movie detail request and download flow stay responsive.
- Cover local catalogue cards, upcoming cards, and TMDb-only search/discovery cards without confusing local database IDs with TMDb IDs.
- Show real TMDb user review excerpts when available, with safe text rendering and source links.
- Preserve the existing trailer, watchlist, request/availability, season, episode, and per-file download actions.

## Non-goals

- Do not invent or relabel the database `rating` column: its provider is not recorded consistently.
- Do not add written review submission or a new review database; the existing user capability is a star rating.
- Do not change download URLs, file identity, search, or other catalogue screens.

## Constraints and behavior

- IMDb and TMDb scores are optional. OMDb supplies the IMDb score only when an IMDb ID and `OMDB_API_KEY` are available; TMDb supplies its score, vote count, and review snippets when a TMDb ID and API key are available. Local catalogue IDs and explicit TMDb IDs use distinct route shapes.
- Provider lookups use short timeouts, bounded concurrency, and a short failure cache. The client renders the movie immediately and fills the Verdict/reviews independently.
- The Verdict is the arithmetic mean of available provider scores, rounded to one decimal. It never uses the unsourced database rating or the separate FlimfyBox star average. It reports how many provider scores contributed. The watch cue is a deterministic label derived from that average, not a promise or a fabricated audience signal.
- No score renders as “Not enough data”; missing one provider is shown as unavailable rather than as zero.
- Download rows retain the original movie/file IDs and handlers. Best available resolution can be identified from the supplied quality text only.
- Desktop uses a cinematic full-width hero and balanced content columns. On small screens downloads precede the long-form story/reviews. Respect reduced-motion preferences.

## Approaches considered

1. Restyle only the current markup. Lowest risk, but it cannot distinguish IMDb/TMDb or produce a defensible combined score.
2. Put one blended score in the hero. Visually strong, but it hides source availability and can mislead when the inputs are missing.
3. Build a source-aware cinema detail page: separate IMDb/TMDb cards, transparent Flimfy Verdict, tidy downloads, and sourced TMDb review excerpts. **Chosen** because it looks distinctive while keeping each score and fallback honest.

## Design calls

- Give the Verdict its own dark, high-contrast card so it stays distinctive in either app theme; use the existing typography, a violet-to-teal accent, and subtle entrance/score transitions. Respect reduced-motion preferences and do not copy another streaming service's page.
- Keep the current user star-rating workflow, but give it a dedicated community panel beside the external-provider ratings.
- Name the written-review area “TMDb community reviews” and link back to TMDb; show a calm empty state when no reviews are returned.
- Build the score presentation as a small reusable frontend module so its calculation and missing-data behavior can be tested independently.

## Acceptance checks

- Opening a detail page never waits for TMDb/OMDb; provider outages produce partial or empty states without breaking downloads.
- Valid IMDb and TMDb values combine to the documented one-decimal arithmetic mean; invalid or absent values are excluded.
- Review author/content are rendered as text, provider URLs are validated, and unavailable reviews do not show fabricated content.
- Every download button still calls the existing handler with its original movie and file IDs.
- Desktop and mobile detail layouts remain usable, keyboard-visible, theme-aware, and reduced-motion friendly.
