# Runbook: DLQ depth climbing

## Symptoms

- Grafana "PolyGlider Resilience" dashboard, **Queue depths (dlx / needs-attention)** panel trending up.
- Prometheus alert `DlqDepthHigh` fires: `polyglider_dlq_depth{queue="dlx.orders.placed"} > 50` for 2 minutes.
- `polyglider_permanent_failures_total` or `polyglider_transient_failures_total` rate increasing in the **Failure rate by classification** panel.

## Diagnosis steps

1. Check both queue depths, not just `dlx.orders.placed` — `needs-attention.orders.placed` is the one that actually needs a human:
   ```bash
   curl -s -u guest:guest http://localhost:15672/api/queues/%2F/dlx.orders.placed | jq '.messages'
   curl -s -u guest:guest http://localhost:15672/api/queues/%2F/needs-attention.orders.placed | jq '.messages'
   ```
2. If `dlx.orders.placed` is climbing but `needs-attention.orders.placed` is flat, the `DlqReprocessor` is keeping up (or messages are still mid-retry across the `dlq-reprocess.orders.placed.<tier>` queues) — not yet an incident.
3. If `needs-attention.orders.placed` is non-zero, `NeedsAttentionDepthNonZero` will also be firing — every message there has already exhausted the reprocessor's retry budget (`DlqReprocessor.defaultRetryPolicy`, 3 retries) or was permanently unprocessable on the first attempt. Pull recent reasons:
   ```bash
   docker logs <scala-container> 2>&1 | grep -E "DLQ reprocess (exhausted|hit a permanent failure)" | tail -50
   ```
4. Cross-check `polyglider_circuit_breaker_state{name="postgres-write"}` — if the breaker is open, the climb is a symptom of the Postgres-outage scenario (see the circuit-breaker-open runbook entry below), not an independent DLQ problem.

## Remediation

- **If the breaker is open / Postgres is down:** fix Postgres first (see next entry). The reprocessor will drain `dlx.orders.placed` on its own once writes succeed again — no manual requeue needed.
- **If `needs-attention.orders.placed` has messages and Postgres is healthy:** each message has a `x-last-failure-reason` header (set by `DlqReprocessor.withReprocessAttempt`) recording why it was escalated. Inspect via the management UI's "Get messages" (use "Requeue" = no, so you don't consume it accidentally) or:
  ```bash
  curl -s -u guest:guest -X POST http://localhost:15672/api/queues/%2F/needs-attention.orders.placed/get \
    -H 'content-type: application/json' \
    -d '{"count":10,"ackmode":"ack_requeue_true","encoding":"auto"}' | jq '.[].properties.headers'
  ```
  Decide per-message: fix-and-replay (see the malformed-payload runbook entry) or discard if it's a known-safe duplicate.
- `dlq-reprocess.orders.placed.<tier>` queues with non-zero depth are just messages waiting out their TTL before redelivery — this is expected and self-resolving, not something to intervene on.

## Verification

- `polyglider_dlq_depth{queue="dlx.orders.placed"}` returns to its steady-state baseline (ideally 0) and stays there for at least one full alert evaluation window (2 minutes).
- `needs-attention.orders.placed` depth is 0, or every remaining message has been explicitly triaged (replayed or discarded).
- `polyglider_messages_processed_total`'s rate has returned to its pre-incident baseline.
