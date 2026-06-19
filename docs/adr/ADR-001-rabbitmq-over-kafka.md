# ADR-001: RabbitMQ over Kafka for the order-placed event bus

**Status:** Accepted

## Context

The gateway needs to hand off `OrderPlaced` events to the Scala processing engine asynchronously, with retry-with-backoff and a dead-letter path for messages that can't be processed. The two realistic broker choices were RabbitMQ and Kafka.

## Options considered

1. **Kafka** — partition-based log, strong replay/retention story, widely used for high-throughput event streaming. Per-partition ordering, consumer groups for horizontal scaling.
2. **RabbitMQ** — topic/fanout exchanges with per-message routing, native TTL-based delayed redelivery (used here for backoff tiers), native dead-letter exchanges, simpler single-node operational model.
3. **In-process queue only** (no broker) — rejected outright: it can't survive a gateway restart and gives up cross-service durability entirely, which defeats the purpose of decoupling the two services.

## Decision

RabbitMQ. The retry-with-backoff and DLX mechanisms this system relies on (`RetryPolicy`-driven per-tier queues with `x-message-ttl` + `x-dead-letter-exchange`, see `RabbitConsumer.scala`) map directly onto RabbitMQ primitives with no in-process scheduler needed — the broker handles the timing. Building the equivalent on Kafka would mean either rolling a custom delay-queue pattern (re-publish-with-delay topics, or a side timer store) or pulling in a dedicated component, since Kafka has no native per-message delay/TTL/DLX concept. The fanout/topic exchange model is also a closer fit for the actual traffic pattern here — single producer, single logical consumer group, exchange-based routing for `orders.placed` vs `dlx.orders.placed` vs `needs-attention.orders.placed` — than Kafka's partition-and-consumer-group model, which is built for much higher throughput and multiple independent consumer groups replaying the same log.

## Consequences

- **Gained:** retry backoff tiers and DLX routing for free from broker features (no custom scheduler); simpler single-broker operational model (one `docker-compose` service, one management UI); exchange-based routing already cleanly separates the three queues used today.
- **Given up:** Kafka's log-based replay (re-consuming historical messages by resetting an offset) isn't available — RabbitMQ queues are destructive-once-acked. If a future requirement needs reprocessing the full order history rather than just current DLQ contents, that's a gap; `processed_events` dedup partially compensates by making reprocessing safe if a replay mechanism is added later.
- **Given up:** Kafka's much higher sustained-throughput ceiling. Not a concern at this system's current scale (a single C# gateway publishing, one Scala consumer group with 4 worker fibers).
- Retention here is "until acked," not "until a configured time window" — there's no equivalent of Kafka's log retention for audit/replay purposes beyond what's stored in Postgres.
