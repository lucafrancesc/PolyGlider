# SLOs and error budgets

**Status:** Defined and validated against the chaos-testing scenarios in `tools/chaos/`. Kept alongside `docs/resilience-design-doc.md` (the failure-handling mechanisms these SLOs measure) and the dashboards/alerts in `observability/` (where they're tracked).

## Why

Dashboards (`observability/grafana/dashboards/polyglider-resilience.json`) show that metrics exist — message rates, failure counts, queue depths. They don't say whether the system is currently *meeting its reliability target*, or how much room is left before it doesn't. An SLO with an error budget turns "the DLQ depth panel went up" into "we've burned 40% of this hour's DLQ-depth budget" — a number that says how worried to be, not just that something moved.

## The three SLOs

### 1. Processing latency

**Target:** ≥99% of orders, over a rolling 1-hour window, are acked by the Scala consumer within 5 seconds of the gateway's recorded order timestamp.

**Measured via:** `polyglider_order_processing_duration_seconds` (new histogram, `Metrics.scala` / `RabbitConsumer.recordProcessingLatency`) — computed as `now - order.timestamp` at the point of a successful ack. This is not a true distributed-trace span duration; it relies on the gateway's and engine's clocks being reasonably in sync, true for this project's single-host/Docker-Compose deployment, and would need real OTel span-duration export instead in a multi-host deployment where that assumption doesn't hold (see `Metrics.scala`'s comment on this metric).

**Recording rule:** `slo:processing_latency:compliance_ratio_1h` (`observability/prometheus/slo-rules.yml`).

**Validated:** a 30-order steady-state run (`./tools/smoke-test.sh 30`, no chaos active) measured a mean processing latency of ~0.22s (`polyglider_order_processing_duration_seconds_sum / _count` = 6.668s / 30), with every single order landing in the histogram's `le="0.5"` bucket — i.e. 100% compliance with enormous headroom under normal operation. The 5s target is deliberately loose relative to that baseline: it's sized to absorb a brief GC pause or broker hiccup without false-alarming, not to be a tight bound on best-case latency.

### 2. DLQ depth (`dlx.orders.placed`)

**Target:** ≥99.9% of the time, over a rolling 1-hour window, `dlx.orders.placed` depth stays at or below 50 messages.

**Measured via:** the existing `polyglider_dlq_depth{queue="dlx.orders.placed"}` gauge — this SLO formalizes the same threshold the pre-existing `DlqDepthHigh` alert already uses (`observability/prometheus/alerts.yml`), as a rolling-window compliance ratio rather than a single instantaneous breach.

**Recording rule:** `slo:dlq_depth:compliance_ratio_1h`.

**Validated:** a 160-second `postgres-kill` chaos run (`./tools/chaos/chaos.sh postgres-kill 160`) with orders posted throughout the outage produced 42 transient failures and 42 eventual successes — `dlx.orders.placed` depth stayed at 0 throughout; every retry succeeded within the main consumer's own 5-tier backoff budget once Postgres came back, without ever needing to escalate to the DLQ at all. This SLO is meant to catch outages that *do* spill into the DLQ (longer or repeated outages, or messages that exhaust the main consumer's retry budget) — see `docs/postmortems/2026-06-18-dlq-reprocessor-retry-before-escalate.md` for measured DLQ-reprocessor recovery/escalation behavior at longer outage durations (~200s recovers within budget, ~260s exhausts it and escalates).

### 3. Circuit breaker (`postgres-write`) open time

**Target:** the `postgres-write` circuit breaker spends no more than 60 seconds in the `open` state, over a rolling 1-hour window.

**Measured via:** `polyglider_circuit_breaker_state{name="postgres-write"}` (existing gauge) — this SLO formalizes the same threshold the pre-existing `CircuitBreakerOpenTooLong` alert uses (which fires on a single continuous open period over 60s; this SLO instead tracks *cumulative* open time across possibly-multiple trips in the window).

**Recording rule:** `slo:circuit_breaker_open:seconds_1h`.

**Validated:** the same `postgres-kill 160` run tripped the breaker open at 5 consecutive failures, then closed again automatically 44.9 seconds later (16:39:37.781 → 16:40:22.728 in engine logs) once Postgres was reachable again and a probe call succeeded — within the 60s budget for a single trip. Note this took roughly 4 minutes of outage to even trip (not the ~150s `docs/postmortems/2026-06-18-postgres-outage-circuit-breaker-unreachable.md` predicted) — the postmortem's number predates the `ch.basicQos(workerCount)` fix and was re-derived for a different prefetch setting; the exact trip latency depends on HikariCP's connection-acquisition timeout (30s, unchanged) and how many of the 4 worker fibers happen to be mid-attempt when Postgres goes down, so it varies run to run. The 60s/hour *open-time* budget targets how long a trip lasts once it happens, not how fast it happens — those are different questions, and this SLO only covers the former.

## Error budget alerting

`observability/prometheus/alerts.yml`'s `DlqDepthHigh` and `CircuitBreakerOpenTooLong` already give fast, real-time detection for SLOs 2 and 3 above (an instantaneous threshold breach pages immediately; the SLO's rolling-window framing is for trend/budget tracking on top of that, not a replacement for it). SLO 1 (processing latency) had no alert at all before this — `ProcessingLatencySLOBudgetBurn` (same file) fires when `slo:processing_latency:compliance_ratio_1h` drops below 99% for 5 minutes sustained.

This project does not implement Google SRE-style multi-window, multi-burn-rate alerting (e.g. separate fast-burn/slow-burn alerts at different sensitivities) — that's sized for a 28/30-day error budget tracked across an organization, disproportionate for three SLOs measured over a 1-hour rolling window on a single-host local stack. A single threshold-plus-`for:` alert, consistent with the project's two existing alerts, is the right level of mechanism here.

## Known limitations

- **Cold-start skew:** all three recording rules use `[1h:]` subqueries. A metric that has existed for less than an hour (e.g. right after `sbt run` restarts, or right after the circuit breaker's *first-ever* state transition — `polyglider_circuit_breaker_state` has no value at all until that happens) gets averaged over whatever shorter history actually exists, not diluted across a full hour of assumed-good samples. This inflates apparent badness immediately after a restart and was directly observed while validating this doc: `slo:circuit_breaker_open:seconds_1h` read ~250s shortly after the chaos run above, despite the actual open period being ~45s, because the breaker's whole metric history at that point was only a few minutes long. This self-corrects once the process has run continuously for over an hour; it is a property of querying a short-lived process, not a bug in the recording rules.
- **Latency metric depends on synced clocks** between the gateway (sets `timestamp`) and the engine (computes the delta) — acceptable for this project's local/single-host deployment, not a general-purpose distributed latency measurement.
- **DLQ depth and circuit breaker recording rules assume one engine instance.** `max by (queue)` / `max by (name)` collapse the scrape `instance` label so stale or duplicate scrape targets (e.g. switching between host-mode and containerized run modes against the same Prometheus, as happened while developing this doc) don't double-count. If the engine were ever run as multiple concurrent replicas, this would silently report only the max-depth/most-recently-open replica rather than a true aggregate — not a concern today since the engine has no multi-replica run mode (unlike the gateway, which scales via `--scale gateway=N` in containerized mode).
