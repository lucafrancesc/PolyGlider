# Runbook: Malformed payload in DLQ

## Symptoms

- A message sits in `needs-attention.orders.placed` (malformed payloads are classified `PermanentFailure` by `ProcessingFailure.classify` and escalate immediately — they never spend time in `dlq-reprocess.orders.placed.<tier>`, see `docs/postmortems/2026-06-18-poison-pill-and-duplicate-handling.md`).
- `polyglider_permanent_failures_total` incremented.
- Log line: `DLQ reprocess hit a permanent failure for eventId=<id-or-"unknown">; escalating immediately` (or, on the main path before it ever reached the reprocessor, `Permanent failure processing message eventId=...; routing to DLX`).
- `eventId` in the log may literally be the string `"unknown"` — `RabbitConsumer.eventIdOf`/`DlqReprocessor`'s equivalent fall back to that when the body can't even be parsed enough to extract `eventId`, which is itself diagnostic (the JSON is broken badly enough that even partial parsing failed).

## Diagnosis steps

1. Pull the message body without acking/discarding it, to inspect the actual bytes:
   ```bash
   curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
     -H 'content-type: application/json' \
     -d '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '.'
   ```
2. Check the `x-last-failure-reason` header (set by `DlqReprocessor.withReprocessAttempt`) — it contains the original `circe.ParsingFailure`/`DecodingFailure` message, which usually pinpoints the exact malformed field or truncation point.
3. Confirm it's genuinely malformed JSON/schema, not a transient misclassification — try parsing the body locally:
   ```bash
   echo '<payload-bytes>' | jq '.'
   ```
   If `jq` parses it fine but `OrderPlaced`'s decoder still rejected it, the issue is a schema mismatch (missing/wrong-typed field) rather than broken JSON — check against the `OrderPlaced` case class and the `order-api.json` contract.

## Remediation

- **Fix-and-replay** (when the intended order is recoverable from the body, e.g. a known client sent a slightly-off schema that can be corrected): manually publish a corrected payload to `orders.exchange` with routing key `orders.placed`, reusing the original `eventId` so the dedup table doesn't matter either way:
  ```bash
  curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/orders.exchange/publish \
    -H 'content-type: application/json' \
    -d '{"properties":{},"routing_key":"orders.placed","payload":"<corrected-json>","payload_encoding":"string"}'
  ```
  Then ack/discard the original malformed message from `needs-attention.orders.placed`.
- **Discard** (when the payload is unrecoverable garbage, e.g. truncated mid-stream, or came from a chaos-test/bad-actor source rather than a real client): ack-without-requeue via the management UI ("Get messages" with requeue=false) or:
  ```bash
  curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
    -H 'content-type: application/json' \
    -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}'
  ```
- If malformed payloads are arriving repeatedly from the same source, that's a client bug, not a one-off — the gateway's own input validation (`Program.cs`, 400 on empty/oversized `sku`, non-positive `quantity`) only catches malformed *HTTP requests*; a malformed message in the DLQ means it bypassed the gateway entirely (chaos tooling does this deliberately via `publish_chaos_message.py`) or the gateway's serialization itself produced bad output (a real bug, worth its own investigation).

## Verification

- The corrected/replayed message is confirmed in the ledger (`mcp__polyglider-inventory__get_sku_quantity` or a direct query) with the expected `qty`/`order_count` delta.
- `needs-attention.orders.placed` depth returns to 0 (or to the count of remaining, still-untriaged messages).
- No duplicate-processing side effects from the replay — since the original `eventId` is reused, `processed_events` dedup protects against double-counting if the original somehow also got processed.
