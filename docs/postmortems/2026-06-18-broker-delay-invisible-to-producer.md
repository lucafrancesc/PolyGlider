# Postmortem: a paused broker is invisible to both the gateway and its own health check

**Date:** 2026-06-18
**Scenario:** `tools/chaos/chaos.sh broker-delay 15`
**Severity:** Medium — no data loss, but two independent signals that should have caught this didn't.

## What we did

Paused `polyglider_broker` (`docker pause`, which freezes every process in the container via the cgroup freezer but leaves its network namespace and existing TCP connections intact) for 15 seconds. During the pause, fired 6 `POST /api/orders` requests two seconds apart and called `GET /health` partway through.

## What happened

- All 6 requests returned `202 Accepted`.
- The gateway logged `Published to RabbitMQ: eventId=...` for all 6 — while the broker was still paused.
- `GET /health` returned `{"status":"healthy","rabbitmq":"ok",...}` while the broker was paused.
- Once the broker was unpaused, all 6 messages arrived at the Scala consumer in a single burst (timestamps within 100ms of each other) and were processed correctly. `polyglider_dlq_depth` and the RabbitMQ queue depth both ended at 0. No data was lost or duplicated.

## Why

Two independent reasons converged to hide a 15-second broker outage entirely:

1. **`RabbitMqPublisherWorker.BasicPublishAsync` doesn't use publisher confirms.** A `docker pause` freezes the broker process but not the kernel's TCP stack for that network namespace — the existing AMQP connection's socket can still accept writes into its send buffer without anything on the other end acknowledging or even reading them. `BasicPublishAsync` returning successfully only means the bytes were handed to the local TCP stack, not that RabbitMQ received or routed them. For 6 small messages over 15 seconds, the send buffer never filled up, so the publish path never even noticed.

2. **`RabbitMqProbe.IsReachableAsync` opens a brand-new `IConnection` on every health check** rather than checking the state of the connection actually used for publishing. A fresh AMQP connection attempt against a paused broker should eventually time out (the probe has a 3s `CancelAfter`), but in this run it returned `true` within that window — likely because the TCP-level handshake can complete via the kernel's accept backlog before the (frozen) broker process ever calls `accept()`, and the probe doesn't appear to wait long enough past that to detect the AMQP protocol handshake never completing. Either way, the health check is structurally checking the wrong thing: a new connection's reachability, not whether the *production* publish path is making progress.

## What recovered automatically

Everything, once the broker resumed. RabbitMQ buffered the connection's outstanding data and delivered it the moment the process unfroze; no message was dropped, duplicated, or required republishing.

## What needed manual intervention

Nothing — which is exactly the gap. A real 15-second broker hiccup (or one considerably longer) would look identical from the outside: `202`s everywhere, a green `/health`, and a quiet Grafana dashboard, right up until the gateway's in-process `Channel<T>` (10k capacity) actually fills up. There is currently no signal between "everything's fine" and "the buffer is full and we're returning 503s."

## Gaps found / what we'd change

- **Add publisher confirms** to `RabbitMqPublisherWorker` (`ConfirmSelect` / `WaitForConfirmsOrDie` or the async equivalent) so a publish that the broker hasn't actually acknowledged doesn't get logged as success and doesn't return `202` to the client.
- **Make the health check probe the actual publish path's connection state** (e.g. expose `IConnection.IsOpen` from `RabbitMqPublisherWorker` to the health check) instead of opening an unrelated connection. A new connection succeeding or failing says nothing about whether in-flight publishes are stuck.
- **Surface gateway-side buffer pressure as a metric**, not just `bufferUsed` in the `/health` JSON body. Right now the only way to notice a slow broker is to poll `/health` and read a number out of the response body; there's no Prometheus series for it and therefore no alert possible. Worth tracking as a follow-up for #38 (SLOs) once the gateway has its own `/metrics` endpoint.
