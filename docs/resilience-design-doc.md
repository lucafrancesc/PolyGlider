# Design doc: resilience decisions (Phases 1-4)

**Status:** Reflects the system as built. Kept alongside the chaos-testing postmortems (`docs/postmortems/`) and the per-decision ADRs (`docs/adr/`), which go into more depth on any single decision than this doc does.

## Problem

A message can fail to process for reasons ranging from "the bytes are garbage" (never going to succeed) to "Postgres is briefly unreachable" (will succeed on its own once the outage clears). Treating every failure the same way means either retrying forever on doomed messages or giving up immediately on recoverable ones. Phases 1-4 of this project built, in order: (1) basic retry-with-backoff for the main consumer, (2) a circuit breaker around the Postgres write path, (3) an automated DLQ reprocessor with its own bounded retry budget, and (4) the failure-classification rules that route between all of the above. Each decision below states the options considered and the trade-off picked; see the linked ADR for the fuller writeup of any single one.

## Decision 1: Backoff strategy (main consumer retries)

**Options considered:** (a) fixed-delay retry, same wait every tier; (b) exponential backoff with jitter (chosen); (c) no in-process retry at all, route every transient failure straight to a human.

**Decision:** Exponential backoff via per-tier RabbitMQ queues (`RetryPolicy.default`: 5 max retries, 1s base delay, 3x multiplier, up to 250ms jitter — `delayFor(tier) = baseDelay * multiplier^(tier-1)`). Each tier is a real RabbitMQ queue with `x-message-ttl` + `x-dead-letter-exchange`, so the broker handles the timing — no in-process scheduler. Jitter exists specifically so that many messages failing at the same instant (e.g. all in-flight requests during a Postgres blip) don't all redeliver in lockstep and re-trigger the same failure simultaneously.

**Trade-off:** Fixed delay (option a) is simpler to reason about but wastes time on outages that resolve quickly (every message waits the same fixed period regardless of tier) and hammers the dependency at a predictable cadence if it's still down. No retry (option c) was rejected outright — it would turn every transient blip into a manual-triage incident, defeating the point of distinguishing transient from permanent at all (see Decision 3).

## Decision 2: Circuit breaker thresholds

**Options considered:** (a) low `max-failures` (2-3), trips almost immediately; (b) higher `max-failures` (5, chosen), assumes failures arrive in a burst from concurrent workers; (c) failure-rate-based tripping instead of a consecutive count.

**Decision:** `max-failures = 5`, `reset-timeout = 30s` (see ADR-005 for the full writeup). Chosen assuming the 4 worker fibers would each independently hit a failing Postgres call during an outage, accumulating 5 consecutive failures quickly.

**Trade-off, and what we learned:** Chaos testing (`docs/postmortems/2026-06-18-postgres-outage-circuit-breaker-unreachable.md`) found this assumption didn't hold — at the time, `ch.basicQos(1)` serialized broker delivery to one in-flight message regardless of worker count, so failures arrived one every ~30s (gated by HikariCP's connection timeout), meaning a real outage needed ~150s before the breaker would trip, not the fast response the threshold was tuned for. The prefetch was subsequently fixed to `ch.basicQos(workerCount)` so failures can again arrive concurrently across all 4 fibers, restoring the assumption the threshold depends on — but the threshold itself (5/30s) has not been re-validated against the corrected prefetch setting. This is the clearest example in the system of a value chosen by reasoning alone turning out wrong under real conditions, and chaos testing being what caught it rather than code review.

## Decision 3: Transient vs permanent classification

**Options considered:** (a) a flat exception-type allow-list; (b) SQLState-class-based rules layered on top of type-based rules for non-SQL errors (chosen); (c) retry everything a fixed number of times and let the retry budget be the only safety net.

**Decision:** `ProcessingFailure.classify` (see ADR-004) distinguishes by SQLState class within `SQLException` — connection/availability errors (classes `08`/`53`/`57`/`58`) are transient, integrity violations (class `23`) are permanent — rather than treating all `SQLException`s the same way. Unrecognized errors default to permanent (fail closed into manual triage rather than risk an infinite retry loop).

**Trade-off:** Option (c) was rejected because it's measurably wasteful — verified live in `docs/postmortems/2026-06-18-poison-pill-and-duplicate-handling.md`, where a malformed payload reached `needs-attention.orders.placed` in milliseconds under the chosen classification, versus what would have been minutes of futile retries (5 tiers, up to ~243s cumulative at the default backoff) under option (c). The cost of option (b) is that classification quality depends entirely on SQLState coverage being complete and correct — an unmapped Postgres error code silently defaults to permanent, which is safe (fails closed) but could over-escalate a recoverable condition.

Note: the postmortem's duplicate-`eventId` example (PK violation on `processed_events` → permanent → DLX) reflects behavior *at the time it was written*. #78 since changed the dedup insert to `ON CONFLICT (event_id) DO NOTHING`, so duplicates are now an idempotent no-op (ack, no escalation) rather than a classified failure; the malformed-payload example above is unaffected and still demonstrates the permanent-failure path live.

## Decision 4: Retry-before-escalation count (DLQ reprocessor)

**Options considered:** (a) no automated DLQ reprocessing at all — every nacked message waits for a human (the system's original design point, per #36's context); (b) a separate, bounded retry budget for the DLQ reprocessor distinct from the main consumer's (chosen); (c) reuse the main consumer's retry budget/count for DLQ messages too, rather than tracking a separate count.

**Decision:** `DlqReprocessor.defaultRetryPolicy` (3 max retries, 10s base delay, 2x multiplier, up to 1s jitter, separate `x-reprocess-count` header from the main consumer's `x-retry-count`) gives DLQ messages their own bounded retry budget before escalating to `needs-attention.orders.placed`. This turns the DLQ from a dead end requiring a human to look at every single failure into a queue that only needs attention once automated recovery has genuinely been exhausted.

**Trade-off:** Option (c) (sharing a single retry count between the main consumer and the reprocessor) would undercount how many total attempts a message has actually had — a message that already used all 5 of the main consumer's retries before landing in the DLQ would have 0 budget left if the reprocessor reused that count, defeating the reprocessor's purpose entirely. The separate-budget approach (b) means a message gets up to 5 (main) + 3 (reprocessor) = 8 total attempts before a human is involved, at the cost of two retry policies to reason about instead of one. Chaos testing (`docs/postmortems/2026-06-18-dlq-reprocessor-retry-before-escalate.md`) confirmed both outcomes work as designed — recovering within budget for outages up to ~200s, correctly escalating for a 260s outage — and also surfaced that each reprocess attempt pays its own ~30s HikariPool timeout on top of the configured backoff delays, so the "3 retries" advertised in config takes meaningfully longer in wall-clock time against a real outage than the raw `10s+20s+40s=70s` would suggest.

## What replaced "manual triage" as the default outcome

Originally, any nack from the main consumer (transient or permanent) went straight to `dlx.orders.placed` for a human to look at. As of Decisions 3 and 4 above, manual triage is now the outcome only for messages that have exhausted every automated path: permanent failures escalate immediately (Decision 3 — no point retrying), and transient failures get a second, independent retry budget in the DLQ reprocessor before escalating (Decision 4). The README's resiliency table and architecture diagram have been updated to reflect this — `dlx.orders.placed` → `DlqReprocessor` → `needs-attention.orders.placed` is the actual automated path today, not `dlx.orders.placed` → human.
