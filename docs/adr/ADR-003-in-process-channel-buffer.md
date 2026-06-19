# ADR-003: In-process `Channel<T>` buffer in the gateway, not a direct publish on the request thread

**Status:** Accepted

## Context

`POST /api/orders` needs to hand an event off to RabbitMQ. The simplest implementation would call `BasicPublishAsync` directly inside the HTTP request handler and return 202 once the broker confirms. PolyGlider instead writes to an in-process bounded `Channel<OrderPlacedEvent>` (capacity 10,000) and a separate `BackgroundService` (`RabbitMqPublisherWorker`) drains it and publishes to RabbitMQ independently.

## Options considered

1. **Publish directly on the request thread.** Simplest possible code path; the request's latency is exactly the broker round-trip's latency.
2. **In-process bounded `Channel<T>` + background publisher** (chosen). The HTTP handler's only job is to validate the request and enqueue; a single background worker owns the RabbitMQ connection and handles reconnects, backoff, and publishing.
3. **External message buffer** (e.g. a local disk-backed queue, or a sidecar process) — rejected as unnecessary complexity for this system's scale; it solves the same problem `Channel<T>` already solves, with an extra moving part and its own failure modes.

## Decision

Option 2. Decoupling the request thread from the RabbitMQ connection means a broker blip (reconnect, brief unavailability) doesn't directly translate into client-visible request latency or failures — the request only fails if the *buffer itself* is full or near capacity (see ADR for #42's high-water-mark behavior), which is a much rarer condition than "the broker connection happens to be mid-reconnect at this exact moment." It also means the gateway can have exactly one RabbitMQ connection regardless of how many concurrent HTTP requests are in flight, which simplifies connection lifecycle management (one `ConnectionShutdownAsync`/`RecoverySucceededAsync` pair, not N).

The buffer is bounded (10,000) and uses `BoundedChannelFullMode.DropWrite` rather than blocking indefinitely or growing unbounded: a slow/down broker degrades to rejecting new orders with a clear signal (429/503 + `Retry-After`) rather than silently accumulating unbounded memory or making every request hang.

## Consequences

- **Gained:** broker connection issues are isolated to the background worker; HTTP request latency for the success path is just "enqueue, return" rather than "round-trip to the broker."
- **Gained:** a single connection/channel pair for the whole gateway process, with reconnect/backoff logic in exactly one place (`RabbitMqPublisherWorker.ExecuteAsync`'s outer loop).
- **Given up:** a 202 response no longer means the broker has the message — only that it's buffered in-process. If the gateway process crashes before the worker drains the buffer, buffered-but-unpublished orders are lost. `StopAsync` mitigates the *graceful*-shutdown case by draining the buffer before exiting (see #93), but an ungraceful crash (OOM kill, `SIGKILL`) still loses whatever was buffered and unpublished at that moment — a known, accepted trade-off rather than a guarantee of durability across an ungraceful crash.
- **Given up:** publish failures (e.g. broker nacks a message — see ADR-004 and the publisher-confirms work in #72) can no longer be surfaced back to the *original* HTTP caller, since that request has already received its 202 by the time the worker discovers the failure. The worker logs and reconnects, but there's no per-order failure channel back to the client.
