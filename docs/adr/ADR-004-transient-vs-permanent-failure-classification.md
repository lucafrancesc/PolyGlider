# ADR-004: Transient vs permanent failure classification rules

**Status:** Accepted

## Context

Every failure while processing an `OrderPlaced` message needs a binary routing decision: retry with backoff (transient — the same message will probably succeed later) or go straight to the DLX for manual triage (permanent — retrying changes nothing because the problem is the message itself, not the infrastructure). Getting this wrong in either direction is costly: classifying a permanent failure as transient retry-loops a doomed message through all 5 backoff tiers before eventually DLXing it anyway (minutes of wasted time, see #30); classifying a transient failure as permanent sends a perfectly fine message to manual triage for no reason, defeating the entire automated-recovery story.

## Options considered

1. **Exception-type allow-list** — maintain a fixed list of exception classes treated as transient (`java.io.IOException`, `TimeoutException`, etc.), default everything else to permanent.
2. **SQLState-class-based rules for SQL exceptions, layered with type-based rules for everything else** (chosen) — distinguishes within `SQLException` by *why* Postgres rejected the operation, not just that it was a `SQLException`.
3. **Retry everything a fixed number of times regardless of classification, let the retry budget itself be the only safety net** — rejected: this is what #30's postmortem-adjacent investigation found to be wasteful for malformed payloads and duplicate `eventId`s, which can never succeed no matter how many times they're retried (verified in `docs/postmortems/2026-06-18-poison-pill-and-duplicate-handling.md` — both scenarios are classified Permanent and skip retries entirely, escalating to `needs-attention.orders.placed` within milliseconds rather than after minutes of futile backoff).

## Decision

Option 2, implemented in `ProcessingFailure.classify` (`processing-engine-scala/src/main/scala/com/polyglider/consumer/ProcessingFailure.scala`):

- **Permanent:** JSON parse/decode failures (`circe.ParsingFailure`/`DecodingFailure` — the payload itself is malformed bytes, retrying doesn't change them); Postgres integrity-constraint violations, SQLState class `23` (e.g. the `processed_events` primary-key violation on a duplicate `eventId` — a business-rule conflict baked into the input, not an infra blip); anything unrecognized (fail closed into the DLQ rather than risk an infinite retry loop on an error nobody anticipated).
- **Transient:** Postgres connection/availability errors, SQLState classes `08`/`53`/`57`/`58` (connection exception, insufficient resources, operator intervention, system error — all describe Postgres or its environment, not the message); generic `IOException`/`TimeoutException`/`SQLTransientException`; `CircuitBreakerOpenException` (the breaker tripped because of an infrastructure outage, not anything about this particular message — see ADR-005).

SQLState-class granularity (option 2) was chosen over a coarse `SQLException` → always-transient or always-permanent rule (a degenerate case of option 1) because the *same* exception type covers both "Postgres is down" (transient) and "this insert violates a constraint" (permanent) — collapsing those into one bucket would misroute one of the two cases no matter which way it defaulted.

## Consequences

- **Gained:** malformed/duplicate messages reach manual triage in milliseconds instead of after minutes of futile retries (verified live, see postmortem above) — both `polyglider_permanent_failures_total` and queue depth confirm this in practice.
- **Gained:** genuine infrastructure outages (Postgres unreachable, circuit breaker open) get the retry-with-backoff treatment they need, recovering automatically once the outage clears without ever reaching the DLQ (verified live for sustained outages up to ~200s, see `docs/postmortems/2026-06-18-dlq-reprocessor-retry-before-escalate.md`).
- **Given up:** classification quality is only as good as SQLState coverage — a Postgres error code not in either `transientSqlStateClasses` or `permanentSqlStateClasses`, and not one of the explicitly-typed exceptions, silently defaults to Permanent. This fails closed (safe) but could over-escalate a genuinely transient condition that doesn't map to a known SQLState class, sending it to manual triage when a retry would have worked.
- **Given up:** the rules are duplicated in intent (not in code — both consumers call the same `ProcessingFailure.classify`) between `RabbitConsumer` (main path) and `DlqReprocessor` (retry-before-escalate path); a change to the classification rules automatically applies to both, which is the desired behavior, but means the two retry *budgets* (`RetryPolicy.default` vs `DlqReprocessor.defaultRetryPolicy`) are the only place the two paths diverge.
