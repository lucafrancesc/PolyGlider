# Runbook: Duplicate message storm

## Symptoms

Since #78 (`fix(scala): use ON CONFLICT DO NOTHING for the event dedup insert`), a duplicate
`eventId` is a **silent, successful no-op**: the `processed_events` insert affects 0 rows
(`ON CONFLICT (event_id) DO NOTHING`, `DoobieSkuStorage.upsertSku`), the ledger upsert is
skipped, and the message **acks normally**. There is no thrown exception, no Postgres
unique-violation, no `PermanentFailure` classification, and no DLX routing — a duplicate storm
never touches `polyglider_permanent_failures_total` or `needs-attention.orders.placed` at all.

This means a duplicate storm produces **none of the alerting signals** other failure modes do.
The only direct evidence is a log line, and even that requires knowing to look for it:

- Log lines: `Duplicate eventId=<uuid> — skipped (already applied)` (`RabbitConsumer.scala`,
  added in #136 — before that fix the consumer logged the misleading `Stored to ledger` line
  for duplicates too, making them indistinguishable from genuine inserts in the logs).
- **Not** a useful signal: `polyglider_messages_processed_total`. This counter increments
  unconditionally after every successful ack (`RabbitConsumer.scala`, `Metrics.messagesProcessed.inc()`),
  regardless of whether `upsertSku` returned `Applied` or `DuplicateSkipped` — it counts
  *deliveries*, not *distinct events*. A duplicate storm inflates it exactly like a genuine
  order-volume spike would, so it cannot be used to detect or confirm one.
- A real symptom worth watching for: `ledger.qty` / `ledger.order_count` growing slower than
  `polyglider_messages_processed_total`'s rate would suggest, for a SKU receiving an unusually
  high proportion of duplicate deliveries — but this is subtle and easy to miss without already
  suspecting duplicates.

In short: if a duplicate-message storm is happening today, nothing pages anyone and no queue
depth rises. The practical way to notice is grepping logs for the `Duplicate eventId=` line, not
metrics.

## Diagnosis steps

1. Confirm duplicates are actually happening and get a sense of scale:
   ```bash
   docker logs <scala-container> 2>&1 | grep "Duplicate eventId=" | grep -oE "eventId=[0-9a-f-]+" | sort | uniq -c | sort -rn | head -20
   ```
2. Identify the source — common causes in this system:
   - A client retrying `POST /api/orders` after a timeout without an idempotency key (the
     gateway generates a *new* `eventId` per request, so this produces genuinely distinct
     orders, not what this runbook covers — rule this out first).
   - A RabbitMQ-level redelivery: a message was delivered and acked, but the broker redelivers
     it anyway (e.g. a connection blip between processing and the ack reaching the broker) —
     the redelivered copy hits the same `eventId` and is correctly skipped by the dedup insert.
     This is normal, expected behavior, not a bug.
   - A manual/chaos-tooling replay reusing an old `eventId` against an already-processed order
     (e.g. `tools/chaos/publish_chaos_message.py duplicate-message`, or replaying a message
     from `docs/runbook/malformed-payload-in-dlq.md`'s fix-and-replay flow incorrectly).
3. Cross-check ledger integrity — confirm `qty`/`order_count` only reflect the first (applied)
   copy of each duplicated `eventId`, not every delivery:
   ```bash
   psql -h localhost -p 5432 -U postgres -d polyglider_inventory \
     -c "select sku, qty, order_count from ledger where sku = '<affected-sku>';"
   ```
   `order_count` should equal the number of *distinct* `eventId`s applied for that SKU, not the
   number of deliveries (including duplicates) the consumer saw.

## Remediation

- **If duplicates are from broker-level redelivery (the normal case):** no remediation needed.
  This is the dedup protection (`processed_events`, [ADR-002](../adr/ADR-002-exactly-once-via-dedup-table.md))
  working exactly as designed — there is no queue to drain and nothing was double-counted.
- **If duplicates are from a misbehaving upstream system or repeated chaos-tooling runs:** stop
  the source. There's nothing to fix on the ledger side — the dedup table already prevented any
  actual damage — but a sustained storm is still worth tracking down since it's needless load on
  the consumer and the database, even though it's harmless to data integrity.

There is no queue to clear and nothing to discard — unlike a permanent-failure storm, a
duplicate storm never reaches `needs-attention.orders.placed`.

## Verification

- `select count(*) from processed_events where event_id = '<uuid>'` returns exactly 1 for every
  affected `eventId` — confirms the dedup insert is doing its job.
- `ledger.qty` / `ledger.order_count` for affected SKUs match the expected total from distinct
  orders only (reconcile against the count of unique `eventId`s applied, not deliveries seen).
- The rate of `Duplicate eventId=` log lines drops back to zero (or its normal baseline) once
  the source identified in step 2 is addressed.
