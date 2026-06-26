# ADR-007: Ports and adapters (hexagonal) boundary across the gateway and processing engine

**Status:** Implemented

## Context

Several layers already follow the ports-and-adapters pattern — a pure domain interface ("port") with one or more infrastructure implementations ("adapters") that domain code never imports directly:

- `processing-engine-scala/src/main/scala/com/polyglider/storage/SkuStorage.scala` — clean port (trait), with `DoobieSkuStorage` as the Postgres adapter and `CircuitBreakerSkuStorage` as a resilience decorator wrapping the port. The consumer and reprocessor call `upsertSku` without knowing whether the back-end is Postgres, H2 (in tests), or a stub.
- `gateway-api-cs/Services/IOrderPublisher.cs` — clean port, with `ChannelOrderPublisher` as the in-process adapter. The HTTP endpoint enqueues events without any RabbitMQ knowledge.

Two areas do not yet follow this pattern:

- `processing-engine-scala/src/main/scala/com/polyglider/consumer/RabbitConsumer.scala` and `DlqReprocessor.scala` both import `com.rabbitmq.client` directly and interleave domain logic (decode → validate → upsert) with RabbitMQ-specific code (connection factory, channel/queue topology setup, `DefaultConsumer`). Queue and exchange names are also hardcoded literals rather than parameters.
- `gateway-api-cs/Services/RabbitMqPublisherWorker.cs` mixes the domain concern (drain buffered orders, serialize, decide what to publish) with the RabbitMQ adapter concern (connection/channel lifecycle, publisher confirms, reconnect-with-backoff, trace-header injection).

The cost of the fusion: message-handling domain logic cannot be unit-tested without a live (or Testcontainers) RabbitMQ broker, and swapping the transport would require reaching into both domain and adapter code.

## Options considered

1. **Extract ports and adapters for the remaining gaps** — introduce `MessageHandler[A]` (Scala) and `IRabbitMqPublisherAdapter` (C#), refactor consumers and worker to depend only on the port, and move all broker-specific code into adapter implementations. Domain logic becomes independently testable; transport is swappable without touching domain code.
2. **Leave as-is** — acceptable while there is only one transport and the integration path is already covered by Testcontainers. No structural benefit worth the refactor cost.
3. **Full entity-genericity (issue #84, closed `not_planned`)** — generalize storage and message-handling over arbitrary entity types, not just `OrderPlaced`. More scope than warranted now.

## Decision

Option 1. Extract the two remaining gaps:

- **Scala** (`MessageHandler` port): introduced `consumer/MessageHandler.scala` with a trait parameterized over the message type, exposing `decode`, `validate`, `eventIdOf`, and `process`; `OrderPlacedHandler` is the concrete `OrderPlaced` implementation backed by `SkuStorage`. `RabbitConsumer.start` and `DlqReprocessor.start` accept a `MessageHandler[OrderPlaced]` instead of inlining decode/validate/process logic. Queue and exchange names are parameterized in `RabbitConsumer.start`.
- **C#** (`IRabbitMqPublisherAdapter` port): introduced an interface with a single `PublishAsync` operation; connection/channel lifecycle, publisher confirms, reconnect-with-backoff, and W3C trace-header injection live in `RabbitMqPublisherAdapter`; `RabbitMqPublisherWorker` covers the domain concern only (drain channel, serialize, call adapter).

Both extractions follow the precedent already set by `SkuStorage`/`DoobieSkuStorage` and `IOrderPublisher`/`ChannelOrderPublisher`.

## Consequences

- **Gained:** domain logic (decode, validate, process) can be unit-tested with a stub storage and no broker — `MessageHandlerSpec` (Scala) and `PublisherWorkerTests` (C#) exercise the extracted code directly, proving the seam is real.
- **Gained:** in tests and future consumers, the transport adapter can be swapped or mocked without Testcontainers spin-up.
- **Implemented:** both extractions are complete. `MessageHandler[A]` / `OrderPlacedHandler` live in `processing-engine-scala/src/main/scala/com/polyglider/consumer/`; `IRabbitMqPublisherAdapter` / `RabbitMqPublisherAdapter` live in `gateway-api-cs/Services/`.
- **No behavior change:** both refactors are purely structural — no new topology, no new entity type, no change to retry, backoff, or publisher-confirms semantics.
- **Not in scope:** entity-genericity (#84, closed `not_planned`) is explicitly excluded; this ADR documents the pattern without committing to making the pipeline generic over entity types.
