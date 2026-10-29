# Advanced Bot Reliability and Mini App Design

## Intent

Make Telegram searches responsive during ingestion work, prevent one stalled or failed SuperBatch item from stopping later files, make Upcoming degrade honestly when TMDB is slow, and remove mini-app controls that do not perform their advertised action.

## Goals

- Keep the five-minute deadline scoped to movie identity and metadata resolution. Do not impose a whole-file save deadline; keep existing timeouts on individual database and Telegram network operations.
- Preserve per-file checkpoints and continue later files after an individual error.
- Keep transient TMDB failures from looking like a valid empty Upcoming catalogue, and keep the API inside the Mini App's request budget.
- Ensure season selection works with keyboard and screen readers, and every visible detail-page control does what its label says.
- Keep local autocomplete fast and preserve the existing dark/light/system preferences.

## Non-goals

- Replacing the bot with a new queue service or changing the database schema in this pass.
- Promising zero failures when Telegram, TMDB, or the database is unavailable.
- Deploying to Render or editing production data.

## Approaches

1. **Focused changes in the existing stack (selected).** Fix the confirmed timeout, blocking-I/O, upstream-outage, and dead-control paths in place. This is the smallest deployable change and preserves the existing bot and Mini App APIs.
2. **New queue/worker architecture.** This could isolate all work, but it adds deployment, durable-queue, and migration risk before production metrics justify that rewrite.
3. **Audit only.** This avoids code risk but leaves the confirmed save cutoff and slow/failing Upcoming behavior in place.

## Constraints and decisions

- Keep the current Python Telegram bot, Flask API, and browser JavaScript architecture.
- Keep the existing five-minute identity/metadata deadline. Save progress is bounded by individual I/O/database timeouts, not one aggregate save timeout.
- Keep API response fields stable (`status` and `movies`) and add only an optional partial-data indicator.
- Use the existing Node test runner for browser behavior and add Python regression cases for server/batch behavior. The current local Python runtime lacks pytest and the bot's dependencies, so run syntax checks and available Node tests locally; record that Python runtime tests need a project environment.

## Work order

1. Batch save/failure isolation and retry safety.
2. User-facing event-loop and Upcoming latency/reliability.
3. Mini App no-op controls, keyboard operation, labels, and artwork fallback.

## Risks

Removing the aggregate save deadline means a batch can stay processing while its current individual operation completes or times out. This preserves the user's explicit no-save-cap requirement; durable per-file checkpoints and existing operation-level timeouts remain the recovery boundary.
