# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Infrastructure (required before running any service)

```bash
docker-compose up -d   # starts RabbitMQ (:5672, management UI :15672) and Postgres (:5432)
```

Default credentials for both: `guest`/`guest` (RabbitMQ), `postgres`/`postgres` (Postgres). Database name: `polyglider_inventory`.

---

## C# Gateway (`gateway-api-cs/`)

```bash
cd gateway-api-cs
dotnet build
dotnet run --project gateway-api-cs.csproj   # listens on http://localhost:5187
```

**Tests (`gateway-api-cs-tests/`):**
```bash
dotnet test --filter "Category!=Integration"   # unit tests, no Docker needed
dotnet test --filter "Category=Integration"    # Testcontainers RabbitMQ, requires Docker
dotnet test                                    # all tests
```

**RabbitMQ connection** is configured via `IConfiguration`. Override with env vars (double-underscore hierarchy separator):
- `RABBITMQ__HOST`, `RABBITMQ__PORT`, `RABBITMQ__USER`, `RABBITMQ__PASSWORD`

---

## Scala Processing Engine (`processing-engine-scala/`)

```bash
cd processing-engine-scala
sbt run          # starts the consumer
sbt test         # unit tests (H2 in-memory, no Docker required)
sbt "testOnly com.polyglider.ParserSpec"   # run a single test class
```

DB and broker settings live in `src/main/resources/application.conf`. The Scala consumer reads broker config from env vars (`RABBIT_HOST`, `RABBIT_PORT`, `RABBIT_USER`, `RABBIT_PASS`) with fallback to `application.conf`.

---

## Load tester (`tools/load-tester/`)

```bash
cd tools/load-tester
source .venv/bin/activate
locust -f locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:5187
```

---

## Architecture

The system is a polyglot pub/sub pipeline:

```
HTTP client
    → POST /api/orders (C# gateway, :5187)
    → Channel<OrderPlacedEvent> (in-process buffer, 10k cap)
    → RabbitMqPublisherWorker (BackgroundService, auto-reconnect)
    → RabbitMQ exchange "orders.exchange" (topic), routing key "orders.placed"
    → queue "orders.placed"
    → RabbitConsumer (Scala, 4 worker fibers)
    → Postgres ledger (upsert on sku)
```

**Failed messages** (nack without requeue from Scala consumer) route to `dlx.orders.exchange` → `dlx.orders.placed` for manual triage.

### C# gateway internals

- `Program.cs` — minimal API entrypoint; registers DI and maps `POST /api/orders`
- `Services/IOrderPublisher` — abstraction injected into the endpoint (enables mocking in tests)
- `Services/ChannelOrderPublisher` — writes to the `Channel<OrderPlacedEvent>` (non-blocking, drops on full)
- `Services/RabbitMqPublisherWorker` — `BackgroundService` that drains the channel and publishes to RabbitMQ with camelCase JSON serialization and 5s reconnect backoff
- Input validation: `sku` must be non-empty, `quantity` must be positive; both return HTTP 400

### Scala engine internals

- `Main.scala` → `OrderProcessor.process` builds a combined `Resource[IO, Unit]`: transactor → conditional Flyway migrations → `RabbitConsumer.start`
- `RabbitConsumer` — uses raw `com.rabbitmq.client` (not fs2-rabbit, which is a declared but unused dependency); bounded `Queue[IO, Delivery]` (1000) with `workerCount` (default 4) parallel fibers
- `Database` — Doobie + HikariCP; `upsertSku` does `INSERT … ON CONFLICT … UPDATE` (idempotent)
- Flyway migration: `V1__create_ledger.sql` — single table `ledger(sku TEXT PRIMARY KEY, qty BIGINT)`
- Tests use H2 in-memory with a manual insert-if-not-exists pattern (H2 doesn't support the Postgres `ON CONFLICT` syntax)

### Event schema on the broker (camelCase JSON)

```json
{ "eventId": "uuid", "sku": "string", "quantity": 1, "customerId": "uuid", "timestamp": "ISO-8601 UTC" }
```

The gateway generates `eventId` and `timestamp`; clients only send `sku`, `quantity`, `customerId`.
