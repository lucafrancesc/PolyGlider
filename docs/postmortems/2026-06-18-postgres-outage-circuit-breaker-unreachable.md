# Postmortem: Postgres outage never trips the circuit breaker

**Date:** 2026-06-18
**Scenario:** `tools/chaos/chaos.sh postgres-kill <duration>`
**Severity:** Medium — no data loss, but a key resilience control turned out to be unreachable under realistic conditions.

## What we did

Ran `postgres-kill` three times against a live stack while sending order traffic:

| Run | Outage duration | Concurrent requests during outage | Consecutive `SQLTransientConnectionException`s observed |
|---|---|---|---|
| 1 | 25s | 20 sequential (1/s) | 0 |
| 2 | 50s | 25 sequential (1 every 2s) | 1 |
| 3 | 70s | 20 fired concurrently | 3 |

## What happened

All orders were eventually processed correctly once Postgres came back — `polyglider_messages_processed_total` caught up to the expected count in every run, and `polyglider_dlq_depth` stayed at 0. From a correctness standpoint, this worked exactly as designed.

But `polyglider_circuit_breaker_state{name="postgres-write"}` never reported a value in any run — not even `0` (closed) — because the breaker only calls `onStateChange` on a *transition*, and it never transitioned. The `CircuitBreakerOpenTooLong` alert in `observability/prometheus/alerts.yml` had nothing to evaluate.

## Why

Two compounding issues:

1. **HikariCP's default connection-acquisition timeout (30s) is longer than short outages.** In run 1 (25s outage), in-flight writes simply blocked until Postgres came back, then succeeded — no exception was ever thrown, so `ProcessingFailure.classify` never saw anything to classify.

2. **`ch.basicQos(1)` in `RabbitConsumer.scala` limits the broker to delivering one unacked message at a time, regardless of `workerCount`.** The consumer is configured for 4 worker fibers, but the AMQP channel's prefetch count means the broker won't hand over a second message until the first is acked or nacked. So during a Postgres outage, writes are serialized: each blocked attempt takes ~30s before timing out, and the *next* one can't even start until that happens. Over a 70-second outage we observed exactly 3 consecutive failures (⌈70/30⌉ = 3) — not 4, not a burst. **To accumulate the 5 consecutive failures `app.circuit-breaker.max-failures` requires, Postgres would need to be down for roughly 150 seconds** (5 × 30s), regardless of how much traffic is in flight.

The breaker's threshold was tuned assuming failures would arrive in a burst from concurrent workers. They don't — they arrive one every 30 seconds, gated by the prefetch limit.

## What recovered automatically

Everything. Retries succeeded once Postgres returned; no messages reached the DLQ; no manual intervention was needed in any of the three runs.

## What needed manual intervention

Nothing during the test, but that's the problem: a *real* outage shorter than 150s would produce the same silence — no breaker trip, no alert, just slower throughput that's invisible unless someone is watching `polyglider_messages_processed_total`'s rate drop to ~1 every 30s.

## Gaps found / what we'd change

- **`app.circuit-breaker.max-failures` and HikariCP's `connectionTimeout` should be tuned together**, not independently. Either lower the Hikari connection timeout (e.g. 5s) so failures surface faster, or lower `max-failures` to 2–3 so the breaker can trip within a single prefetch-gated failure cycle.
- **`ch.basicQos(1)` should be revisited.** `workerCount` (4 by default) currently has no effect on how fast the broker delivers messages to this consumer — only on how many can be in local processing at once after delivery, which is moot if delivery itself is serialized to one at a time. Worth filing as a follow-up to either raise the prefetch count to match `workerCount` or document that `workerCount` is currently about something else (in-process fan-out from the internal queue, not broker delivery concurrency).
- **The circuit breaker gauge has no initial value.** `polyglider_circuit_breaker_state` only appears in `/metrics` after the first transition, so a Grafana panel or alert has "no data" rather than a clean `0` (closed) baseline from process start. `CircuitBreaker.create` should set the gauge to closed (0) at construction time, not rely on the first failure/recovery event.
