# Runbook: Duplicate message storm

## Symptoms

- A burst of `polyglider_permanent_failures_total` increments with no corresponding rise in `polyglider_dlq_depth{queue="dlx.orders.placed"}` staying elevated for long (duplicates are classified `PermanentFailure` and escalate to `needs-attention.orders.placed` immediately, same fast path as malformed payloads).
- Log lines: `Key (event_id)=(<uuid>) already exists` — a Postgres unique-violation on the `processed_events` primary key (SQLState class `23`, see `ProcessingFailure.classify`).
- `needs-attention.orders.placed` depth rising, with each message's `x-last-failure-reason` header containing the same `already exists` text.
- Crucially: **`polyglider_messages_processed_total` should NOT be incrementing once per duplicate** — only the first copy of any given `eventId` should count.

## Diagnosis steps

1. Confirm this is actually duplicate `eventId`s and not a real spike in distinct orders — pull recent failure reasons and look for repeated UUIDs:
   ```bash
   docker logs <scala-container> 2>&1 | grep "already exists" | grep -oE "event_id\)=\([^)]+\)" | sort | uniq -c | sort -rn | head -20
   ```
2. Identify the source of the duplicates — common causes in this system:
   - A client retrying `POST /api/orders` after a timeout without an idempotency key (the gateway generates a *new* `eventId` per request, so this produces genuinely distinct orders, not what this runbook covers — rule this out first).
   - A RabbitMQ-level redelivery: a message was delivered, processed, but the ack was lost (e.g. a worker crash between `upsertSku` succeeding and `basicAck` completing) — re-delivery hits the same `eventId` and correctly gets rejected by the dedup constraint. This is normal, expected behavior, not a bug.
   - A manual/chaos-tooling replay reusing an old `eventId` against an already-processed order (e.g. `tools/chaos/publish_chaos_message.py duplicate-message`, or a runbook fix-and-replay from the malformed-payload entry done incorrectly).
3. Confirm no ledger corruption — cross-check that `qty`/`order_count` only reflect the first (successful) copy of each duplicated `eventId`:
   ```bash
   psql -h localhost -p 5432 -U postgres -d polyglider_inventory \
     -c "select sku, qty, order_count from ledger where sku = '<affected-sku>';"
   ```
   Compare against `polyglider_messages_processed_total`'s total increment over the same window — it should match the count of *distinct* `eventId`s, not the count of delivery attempts.

## Remediation

- **If duplicates are from broker-level redelivery (the normal case):** no remediation needed — this is the dedup protection (`processed_events`, ADR-002) working exactly as designed. The messages in `needs-attention.orders.placed` are safe to discard (ack-without-requeue) since the original copy already succeeded.
- **If duplicates are from a misbehaving upstream system or repeated chaos-tooling runs:** stop the source. There's nothing to "fix" on the ledger side — the dedup table already prevented any actual damage — but repeated storms add noise to `needs-attention.orders.placed` that still needs manual discarding each time.
- Discard confirmed-safe duplicates from `needs-attention.orders.placed`:
  ```bash
  curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
    -H 'content-type: application/json' \
    -d '{"count":50,"ackmode":"ack_requeue_false","encoding":"auto"}' > /dev/null
  ```
  (Increase `count` to match the storm size; verify each batch's `eventId`s genuinely already exist in `processed_events` before discarding, rather than blindly draining the queue.)

## Verification

- `select count(*) from processed_events where event_id = '<uuid>'` returns exactly 1 for every affected `eventId` — confirms no actual double-insert slipped through.
- `ledger.qty` / `ledger.order_count` for affected SKUs match the expected total from distinct orders only (manually reconcile against the count of unique `eventId`s if in doubt).
- `needs-attention.orders.placed` depth returns to 0.
- `polyglider_messages_processed_total`'s total increment for the incident window equals the number of distinct `eventId`s seen, not the number of delivery attempts.
