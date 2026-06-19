# ADR-005: Circuit breaker thresholds (`max-failures`, `reset-timeout`)

**Status:** Accepted, with a known gap (see Consequences)

## Context

`CircuitBreakerSkuStorage` wraps the Postgres write path (`processing-engine-scala/src/main/scala/com/polyglider/resilience/CircuitBreaker.scala`) to fail fast during a sustained outage rather than letting every in-flight message individually block on a doomed connection attempt. Two numbers govern its behavior: `app.circuit-breaker.max-failures` (consecutive failures before tripping Closed → Open) and `app.circuit-breaker.reset-timeout-ms` (cooldown before Open → Half-Open allows one trial call through). Current configuration (`application.conf`): `max-failures = 5`, `reset-timeout-ms = 30000`.

## Options considered

1. **Low `max-failures` (e.g. 2-3), tuned to trip almost immediately on any outage.** Minimizes wasted retry time per message but risks tripping on transient, self-resolving blips (a single slow query, a brief network hiccup) that don't warrant failing fast.
2. **High `max-failures` (5, chosen), tuned assuming failures arrive in a burst from concurrent workers.** Intended to absorb occasional unrelated failures without tripping, only opening when failures are clearly sustained.
3. **Failure-rate-based tripping** (e.g. "50% of the last N calls failed") rather than consecutive-count — rejected as unnecessary complexity for a single-operation breaker guarding one write path with modest concurrency; a sliding-window failure rate is overkill when the call pattern is already serialized (see Consequences below).

## Decision

Option 2: 5 consecutive failures trips the breaker; 30 seconds of Open state before a Half-Open trial call. This assumes failures would arrive in a burst proportional to `workerCount` (4 worker fibers, each independently hitting the failing Postgres call), so 5 consecutive failures should accumulate quickly during a real outage.

## Consequences

- **This assumption turned out to be wrong in practice**, discovered via chaos testing (`docs/postmortems/2026-06-18-postgres-outage-circuit-breaker-unreachable.md`): `ch.basicQos(workerCount)` sets the AMQP prefetch to match worker count, but at the time of that postmortem the channel was configured with `basicQos(1)`, serializing broker delivery to one in-flight message regardless of `workerCount`. Combined with HikariCP's default 30s connection-acquisition timeout, failures arrived one every ~30 seconds, not in a burst — so reaching 5 consecutive failures required roughly **150 seconds** of sustained outage, not the fast trip the threshold was tuned for. The breaker's gauge (`polyglider_circuit_breaker_state`) never transitioned during any of three test runs up to 70s, so `CircuitBreakerOpenTooLong` had nothing to fire on.
- **Follow-up applied:** `RabbitConsumer.scala` now sets `ch.basicQos(workerCount)` (prefetch scales with worker count) instead of a fixed `1`, so failures during an outage can arrive concurrently across all 4 worker fibers again, matching the assumption this ADR's threshold was tuned for.
- **Still open:** even with prefetch fixed, `max-failures = 5` and HikariCP's connection timeout still need to be tuned *together*, not independently — the postmortem's recommendation to either lower Hikari's `connectionTimeout` (e.g. to 5s, so failures surface faster) or lower `max-failures` to 2-3 has not yet been re-validated against the corrected prefetch setting. This ADR records the threshold as chosen, but flags that its real-world tripping latency under the current configuration has not been re-measured since the prefetch fix.
- **Gained regardless of the gap above:** once the breaker does trip, it cleanly prevents flooding a downed Postgres with one connection-timeout failure per in-flight message — `protect` fails fast with `CircuitBreakerOpenException` (classified Transient, see ADR-004) without touching the underlying operation at all while Open.
- **Given up:** a single shared breaker instance per named operation (`"postgres-write"`) means there's no per-message-type or per-SKU granularity — any failure on any write trips the same breaker for all writes, which is the intended behavior here (Postgres is either reachable or it isn't) but would need rethinking if a future operation has a meaningfully different failure profile.
