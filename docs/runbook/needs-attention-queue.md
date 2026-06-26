# Runbook: needs-attention.orders.placed non-empty

## Symptoms

- Prometheus alert `NeedsAttentionDepthNonZero` fires: `polyglider_dlq_depth{queue="needs-attention.orders.placed"} > 0` for 5 minutes.
- Grafana "PolyGlider Resilience" dashboard, **Queue depths (dlx / needs-attention)** panel shows non-zero depth for `needs-attention.orders.placed`.
- Every message in this queue has already exhausted all automated retry budgets (main consumer: 5 tiers; DLQ reprocessor: 3 tiers) or was classified as a permanent failure on first attempt (malformed JSON, unsupported schema version, invalid UUID). No further automated recovery will happen.

## Diagnosis steps

1. Check current depth:
   ```bash
   curl -s -u guest:guest http://localhost:15672/api/queues/%2F/needs-attention.orders.placed | jq '.messages'
   ```

2. Inspect the message headers without consuming the message (`ack_requeue_true` peeks, it does not discard):
   ```bash
   curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
     -H 'content-type: application/json' \
     -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '.[].properties.headers'
   ```
   Key headers to read:
   - `x-last-failure-reason` — set by `DlqReprocessor.withReprocessAttempt`; contains the original exception message (e.g. `circe.DecodingFailure`, `InvalidUuidException`, `UnsupportedSchemaVersionException`)
   - `x-reprocess-count` — how many reprocessor attempts were made before escalation (0 = permanent failure on first attempt; 3 = exhausted the reprocessor's retry budget)

3. Find the original `eventId` for log correlation:
   ```bash
   curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
     -H 'content-type: application/json' \
     -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '.[].payload | fromjson | .eventId'
   ```
   Then search engine logs:
   ```bash
   docker logs <scala-container> 2>&1 | grep '<eventId>'
   ```

4. Classify the failure type from `x-last-failure-reason`:
   - **Parse/decode failure** (`circe.ParsingFailure`, `circe.DecodingFailure`, `InvalidUuidException`, `UnsupportedSchemaVersionException`) → permanent, payload is malformed or uses an unrecognized schema version. See `docs/runbook/malformed-payload-in-dlq.md`.
   - **Transient exhausted** (e.g. `PSQLException`, `HikariPool-1 - Connection is not available`) → the reprocessor ran out of retries during a sustained infrastructure failure. Likely safe to replay once the underlying issue (Postgres, network) is resolved.

## Remediation

### Case 1: Payload is malformed or unrecognized schema

Refer to `docs/runbook/malformed-payload-in-dlq.md` for the fix-and-replay vs. discard decision tree.

### Case 2: Transient failure exhausted retries (infrastructure recovered)

Re-submit the order via the `place_order` MCP tool (reuses a new `eventId`, so the dedup table is not a factor):
```
mcp__polyglider-inventory__place_order(sku="<sku>", quantity=<qty>, customer_id="<customerId>")
```

Or manually re-publish to `orders.exchange` with routing key `orders.placed`, reusing the original `eventId` to avoid double-counting if the message was partially processed:
```bash
curl -s -u guest:guest -X POST http://localhost:15672/api/exchanges/%2F/orders.exchange/publish \
  -H 'content-type: application/json' \
  -d '{"properties":{},"routing_key":"orders.placed","payload":"<original-json>","payload_encoding":"string"}'
```

Then ack-without-requeue to remove the original from `needs-attention.orders.placed`:
```bash
curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
  -H 'content-type: application/json' \
  -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}'
```

## Verification

- `polyglider_dlq_depth{queue="needs-attention.orders.placed"}` returns to 0 and stays there for at least one full alert evaluation window (5 minutes).
- If re-published with the original `eventId` and it was already partially processed: `polyglider_permanent_failures_total` increments (the dedup table rejects it as a duplicate), which is the expected safe outcome — the ledger is not double-counted.
- If re-submitted with a new `eventId` via `place_order`: confirm the expected `qty` and `order_count` delta in the ledger:
  ```bash
  # via MCP tool
  mcp__polyglider-inventory__get_sku_quantity(sku="<sku>")
  # or direct query
  psql "$POSTGRES_URL" -c "SELECT sku, qty, order_count FROM ledger WHERE sku = '<sku>';"
  ```
- Alert `NeedsAttentionDepthNonZero` clears within 5 minutes of the queue reaching 0 depth.
