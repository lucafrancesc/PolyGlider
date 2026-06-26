# ADR-005: Circuit breaker thresholds (`max-failures`, `reset-timeout`)

**Status:** Accepted

## Context

`CircuitBreakerSkuStorage` wraps the Postgres write path (`processing-engine-scala/src/main/scala/com/polyglider/resilience/CircuitBreaker.scala`) to fail fast during a sustained outage rather than letting every in-flight message individually block on a doomed connection attempt. Two numbers govern its behavior: `app.circuit-breaker.max-failures` (consecutive failures before tripping Closed → Open) and `app.circuit-breaker.reset-timeout-ms` (cooldown before Open → Half-Open allows one trial call through).

### Trip-time formula

With `workerCount` parallel fibers, failures during an outage arrive in waves of `workerCount` — all workers that are processing a message simultaneously hit the failed Postgres call together. The number of waves needed to accumulate `max-failures` is `⌈max-failures / workerCount⌉`. Each wave completes in one HikariCP connection-acquisition timeout (`app.db.connection-timeout-ms`). So:

```
trip_time_ms ≈ ⌈max-failures / workerCount⌉ × connection-timeout-ms
```

**Current configuration** (`application.conf`): `max-failures = 3`, `reset-timeout-ms = 30000`, `connection-timeout-ms = 5000`, `workers = 4`.

```
trip_time_ms ≈ ⌈3 / 4⌉ × 5000 = 1 × 5000 = ~5s
```

### History

The initial configuration (`max-failures = 5`, HikariCP default 30s timeout) was chosen assuming failures would arrive in a burst from concurrent workers. Chaos testing revealed this was wrong: the channel had `basicQos(1)`, serializing broker delivery to one unacked message at a time regardless of worker count. Failures therefore arrived one every ~30s, requiring ~150s to accumulate 5 consecutive failures — see `docs/postmortems/2026-06-18-postgres-outage-circuit-breaker-unreachable.md`.

**First fix applied** (commit with `basicQos(workerCount)`): restored the concurrency the original threshold assumed — all 4 workers can now receive and process messages simultaneously. But even with prefetch fixed, `max-failures = 5` and the 30s Hikari default still produced a ~60s trip time, far above the <10s target.

**This ADR** records the re-validated configuration that achieves the target.

## Options considered

1. **Lower `max-failures` only (e.g. to 2-3), keep 30s Hikari timeout.** With `workers=4` and `max-failures=3`: `⌈3/4⌉ × 30s = 30s` trip. Better than 150s, still not <10s.
2. **Lower Hikari `connectionTimeout` only (e.g. to 5s), keep `max-failures=5`.** With `workers=4` and `max-failures=5`: `⌈5/4⌉ × 5s = 10s` trip. Borderline on the <10s target.
3. **Lower both: `max-failures=3` and `connectionTimeout=5s`.** `⌈3/4⌉ × 5s = 5s`. Comfortably under the target. Risk of false trips: requires 3 *consecutive* Postgres failures, so a single slow query or brief blip within a single wave still doesn't trip — a true outage (all workers failing in the same wave) does.
4. **Failure-rate-based tripping** — rejected as before; a sliding-window failure rate is unnecessary complexity for a single-operation breaker with consistent call patterns.

## Decision

Option 3: `max-failures = 3`, `app.db.connection-timeout-ms = 5000`, `reset-timeout-ms = 30000`.

- **Trip time:** `⌈3/4⌉ × 5s = ~5s` — a real Postgres outage trips the breaker in one failure wave (5s connection timeout × 1 wave).
- **False-trip risk:** low — a single slow query resolves before the 5s timeout in practice; the 3-failure threshold means one wave of concurrent failures (where all 4 workers time out together) suffices, but an isolated failure on one worker in an otherwise healthy pool doesn't accumulate.
- **Reset:** 30s Half-Open trial before re-closing, unchanged — this is the recommended value for a Postgres dependency that may take tens of seconds to restart.

## Consequences

- **Trip time** drops from ~150s (original `basicQos(1)` + 30s Hikari) to ~5s, which is the target.
- **`reset-timeout-ms = 30000`** is unchanged — appropriate for a database that may take 10-30s to restart after a kill/restart cycle.
- **HikariCP `connectionTimeout` is now explicit** (5s) rather than relying on the 30s default, making the trip-time formula derivable from config without knowing HikariCP internals.
- **Still given up:** a single shared breaker instance per named operation (`"postgres-write"`) means no per-message-type or per-SKU granularity — any write failure trips the same breaker for all writes, which is correct for a single-Postgres-node deployment.
- **Chaos re-validation:** the formula has been analytically validated against the implementation (`CircuitBreaker.scala` trips on `nextFailures >= maxFailures`, so 3 consecutive failures in one 5s wave is sufficient). A live chaos re-run with the new config should confirm the ~5s trip time; if measured values diverge significantly, revisit the assumptions (e.g. whether all 4 workers truly fail in the same wave under the new prefetch setting).
