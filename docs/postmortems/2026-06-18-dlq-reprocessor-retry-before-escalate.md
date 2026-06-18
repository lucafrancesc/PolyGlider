# Postmortem: exercising the DLQ reprocessor's retry-before-escalate path

**Date:** 2026-06-18
**Scenario:** `tools/chaos/chaos.sh dlq-reprocessor-retry <duration>`
**Severity:** None — both outcomes (recover via retry, exhaust and escalate) behaved exactly as designed.

## Why this scenario was needed

The two existing message-injection scenarios (`malformed-payload`, `duplicate-message`, see the [poison-pill postmortem](2026-06-18-poison-pill-and-duplicate-handling.md)) both produce `PermanentFailure`s, which `DlqReprocessor` escalates to `needs-attention.orders.placed` immediately — its own bounded retry-before-escalate path (a separate retry budget from the main consumer's, see `DlqReprocessor.defaultRetryPolicy`) never got exercised by either one.

To get a *transient*-reason message into `dlx.orders.placed` without waiting out all 5 of the main consumer's real backoff tiers (which take minutes), `tools/chaos/publish_chaos_message.py transient-in-dlx` publishes directly to `orders.exchange` with the `x-retry-count` header pre-set to `RetryPolicy.default.maxRetries` (5). The next transient failure the main consumer hits (Postgres unreachable) sees `attemptsSoFar >= maxRetries` and nacks straight to the DLX instead of scheduling another retry — landing a transient-reason message in `dlx.orders.placed` in well under a second instead of minutes.

## What we did

Ran `dlq-reprocessor-retry` three times with increasing Postgres-outage durations, to observe both of the reprocessor's possible outcomes:

| Run | Outage duration | Outcome |
|---|---|---|
| 1 | 8s | Succeeded after 1 retry |
| 2 | 200s | Succeeded after 3 retries (narrowly avoided escalation) |
| 3 | 260s | **Exhausted all 3 retries, escalated to `needs-attention.orders.placed`** |

## What happened

**Run 1 (8s outage):** The reprocessor's first attempt failed (`Connection to localhost:5432 refused`), scheduled tier-1 retry (10s TTL), and by the time that redelivered, Postgres was back — succeeded with `after 1 prior attempt(s)`.

**Run 2 (200s outage):** Each failed attempt's `HikariPool` connection-acquisition timeout (30s, since each attempt is itself blocked waiting on Postgres) compounds with the reprocessor's own backoff TTLs (10s → 20s → 40s). The cumulative time to exhaust all 3 tiers is roughly `3×30s + 10s + 20s + 40s ≈ 160s` before the *next* attempt after exhausting would even fire — 200s wasn't quite enough margin, so the 4th attempt (after the tier-3 40s TTL) landed just after Postgres came back, succeeding with `after 3 prior attempt(s)` instead of escalating.

**Run 3 (260s outage):** With more margin, the 4th attempt landed while Postgres was still down:
```
DLQ reprocess hit a transient failure for eventId=b3d33874-...
DLQ reprocess exhausted for eventId=b3d33874-... after 3 attempt(s)
  (reason=HikariPool-1 - Connection is not available, request timed out after 30000ms ...);
  escalating to needs-attention.orders.placed
```
`needs-attention.orders.placed` depth (and `polyglider_dlq_depth{queue="needs-attention.orders.placed"}`) both went from 0 to 1, and the `NeedsAttentionDepthNonZero` alert (#74) would fire on this in a monitored deployment.

## What recovered automatically

Runs 1 and 2 — the reprocessor's bounded retry budget absorbed a real, sustained Postgres outage without any manual intervention, exactly as #32 intended.

## What needed manual intervention

Run 3's message landed in `needs-attention.orders.placed` for manual triage, as designed — the reprocessor correctly gave up after exhausting its budget rather than retrying forever.

## Gaps found / what we'd change

- **Each reprocess attempt pays a full ~30s HikariPool timeout before the reprocessor's own backoff even starts counting**, because the reprocessor uses the same `Database.transactorResource`/connection pool sizing as the main consumer. This means the reprocessor's "3 retries" advertised in config (`app.reprocessor.max-retries=3`) actually takes ~160s of wall-clock time to exhaust against a real outage, not the `10s+20s+40s=70s` someone reading just the retry-policy config would expect. Worth documenting this compounding effect next to `DlqReprocessor.defaultRetryPolicy`, or revisiting once #69-style HikariCP timeout tuning happens elsewhere.
- **No test in this repo previously exercised this path at all** — it was entirely unverified prior to this scenario, despite being the entire reason `DlqReprocessor` has its own `RetryPolicy` distinct from the main consumer's.
