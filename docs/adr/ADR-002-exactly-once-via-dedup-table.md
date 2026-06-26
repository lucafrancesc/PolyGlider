# ADR-002: Exactly-once semantics via `processed_events` dedup, not idempotent-by-construction consumers

**Status:** Accepted

## Context

RabbitMQ (like virtually every broker) only guarantees at-least-once delivery: a message can be redelivered after a consumer crash between processing and acking, after a nack-and-retry, or after manual requeue from the DLQ reprocessor. Without protection, redelivery of an already-processed `OrderPlaced` event would double-count the order against the ledger (`ledger.qty`, `ledger.order_count`).

## Options considered

1. **Pure idempotent consumer logic** — make `upsertSku` itself naturally safe to call twice with no extra state, e.g. by storing the *set* of contributing event IDs per SKU rather than a running count, and deriving `qty`/`order_count` from `COUNT(DISTINCT event_id)`. Avoids a second table but requires redesigning the ledger schema and makes every read a join/aggregation instead of a row lookup.
2. **Broker-side dedup** (e.g. RabbitMQ's quorum-queue de-duplication, or an external dedup proxy) — rejected: RabbitMQ has no first-class message-ID dedup feature; building one would mean reimplementing option 3 anyway, just outside the database transaction boundary instead of inside it.
3. **At-least-once delivery + a `processed_events(event_id PRIMARY KEY)` table, dedup enforced via the same transaction as the ledger write** — the chosen option (`V2` migration; `DoobieSkuStorage.upsertSku`).

## Decision

Option 3. `upsertSku` performs the `processed_events` insert and the `ledger` upsert in one transaction (Doobie/Postgres `INSERT ... ON CONFLICT`). A duplicate `eventId` hits the `processed_events` primary-key constraint, the whole transaction rolls back, and `ProcessingFailure.classify` treats the resulting unique-violation as a `PermanentFailure` (retrying a duplicate would just reproduce the same conflict) — verified live in `docs/postmortems/2026-06-18-poison-pill-and-duplicate-handling.md`, where two duplicate copies of the same `eventId` were both rejected and the ledger was confirmed not to be double-counted.

This was chosen over option 1 because it keeps the ledger schema simple (a running counter, not a derived aggregate) and keeps the dedup check local to a single insert rather than redesigning every write path. It was chosen over option 2 because RabbitMQ has no equivalent primitive, and building one would just be a worse version of option 3.

## Consequences

- **Gained:** redelivery of an already-processed message is always safe and always cheap (one constraint check, no double-counting), and this is true regardless of *why* the redelivery happened — broker requeue, consumer crash before ack, or manual DLQ reprocessing all hit the same protection.
- **Gained:** the failure path is exercised by the same `ProcessingFailure.classify` machinery as everything else, so duplicates get the same escalation behavior (no retry, straight to `needs-attention.orders.placed`) as other permanent failures, with no special-casing.
- **Given up:** `processed_events` grows without bound (one row per ever-processed event, never pruned) — acceptable at current scale but would need a retention/archival policy if event volume grows large enough for table size to matter. See the retention note below.
- **Given up:** dedup is keyed purely on `eventId`, generated once by the gateway. If a client retries an HTTP request without reusing the same idempotency key client-side, the gateway happily generates a *new* `eventId` and the duplicate sails through as a legitimate new order — this ADR's dedup only protects against *broker-level* redelivery of the same message, not client-level duplicate submission.

## Retention policy

`processed_events` has one row per processed event and is never pruned automatically. Growth rate at the load tester's default settings (100 users, 10 ramp-up, 1-minute run) is on the order of a few thousand rows per test run — negligible. At sustained production-like volumes:

- 1,000 orders/day → ~365k rows/year → ~20 MB/year (index included) — no action needed
- 100,000 orders/day → ~36.5M rows/year → ~2 GB/year — review at this scale

**Threshold for action:** when `pg_total_relation_size('processed_events')` exceeds the Postgres `shared_buffers` setting (default 128 MB on most installs), index pages will no longer fit in the buffer cache and sequential-write throughput begins to fall. At ~1,000 bytes/row (index overhead included), this is roughly 128k rows — reached at ~100,000 orders/day after about half a day.

**Safe archival window:** a row in `processed_events` can be deleted once no in-flight redelivery of that event could still arrive. The worst-case redelivery window is the sum of the main consumer's full retry budget plus the DLQ reprocessor's full retry budget:

- Main consumer: 5 tiers with 3× backoff from 1 s → approximately 1 + 3 + 9 + 27 + 81 = 121 s total delay
- DLQ reprocessor: 3 tiers with 2× backoff from 10 s → approximately 10 + 20 + 40 = 70 s total delay
- Safety margin: 60 s

Total: ~4 minutes 11 seconds. Rows with `processed_at < now() - interval '10 minutes'` are safe to archive with a comfortable margin.

See `docs/runbook/processed-events-archival.md` for the step-by-step archival query.
