# Runbook: RabbitMQ broker unreachable

## Symptoms

- `GET /health` on the gateway returns `503` with `{"status":"unhealthy","rabbitmq":"unreachable",...}`.
- Gateway logs: `RabbitMQ connection lost, retrying in <delay>` (from `RabbitMqPublisherWorker.ExecuteAsync`'s outer catch).
- `POST /api/orders` starts returning `429`/`503` (see #42 — buffer near-capacity or full) as the in-process `Channel<T>` buffer fills up because nothing is draining it.
- On the Scala side: consumer logs show connection errors / the process is stuck in its own reconnect attempt inside `RabbitConsumer.start`'s `Resource.make` block (this blocks engine startup if the broker is down when the engine starts, or surfaces as the engine's `Resource` finalizer/acquire failing if it goes down mid-run).

## Diagnosis steps

1. Confirm the broker is actually down vs. a network/credentials issue:
   ```bash
   curl -s -u guest:guest http://localhost:15672/api/overview | jq '.rabbitmq_version'
   docker ps --filter name=rabbitmq
   docker logs <rabbitmq-container> --tail 100
   ```
2. Check the gateway's buffer occupancy to gauge how much backlog has accumulated and how close to data loss this is (buffer capacity is 10,000, `BoundedChannelFullMode.DropWrite` once full):
   ```bash
   curl -s http://localhost:5187/health | jq '.bufferUsed'
   ```
3. Check whether this is a full broker outage (container down, port unreachable) vs. a partial issue (broker up but a specific vhost/queue misconfigured) — the gateway and Scala engine behave identically either way (both reconnect-loop), but the fix differs.

## Remediation

- **Gateway side:** nothing to do manually. `RabbitMqPublisherWorker.ExecuteAsync` retries with exponential backoff (`ReconnectBackoff.Delay`, base 1s, multiplier 2x, max 30s, up to 1s jitter) indefinitely until the broker is reachable again — orders keep buffering (up to 10,000) in the meantime. If the buffer is at risk of filling before recovery, the only intervention is fixing the broker faster; there's no manual buffer-flush mechanism.
- **Scala side:** `RabbitConsumer.start` and `DlqReprocessor.start` both acquire their connection inside a `Resource.make` block with no internal retry of their own — if the broker is unreachable at startup, the engine process will fail to start and needs to be restarted once the broker is back (`sbt run` / container restart). If the broker goes down *while the engine is already running*, the existing `com.rabbitmq.client` connection will itself attempt automatic recovery (the Java client's default behavior) without requiring a process restart — only a startup-time outage needs a manual restart.
- **Actual broker fix:** restart the RabbitMQ container/service (`docker compose up -d rabbitmq`), or resolve whatever infra issue took it down (disk full from queue backlog, OOM, etc. — check `docker logs` for the actual cause before just restarting blindly).

## Recovery order

1. Bring RabbitMQ back up first.
2. The gateway's `RabbitMqPublisherWorker` reconnects automatically and drains its buffered orders — no restart needed.
3. The Scala consumer's underlying connection also auto-recovers if it was already running (`RecoverySucceededAsync`-equivalent reconnection in the Java client); if the engine process itself had exited (e.g. it crashed or was started fresh during the outage), restart it after RabbitMQ is confirmed healthy.
4. Confirm both `dlx.orders.placed` and `needs-attention.orders.placed` depths didn't climb during the outage (an extended outage could exhaust retry budgets on messages that were already in flight) — see the DLQ-depth-climbing runbook entry if they did.

## Verification

- `GET /health` on the gateway returns `200` with `"rabbitmq":"ok"` and `bufferUsed` trending back toward 0.
- Scala consumer logs show normal `Message received`/`Stored to ledger` activity resuming.
- No unexpected growth in `polyglider_dlq_depth` for either queue once the backlog has been worked through.
