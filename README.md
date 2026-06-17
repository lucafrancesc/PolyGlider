# PolyGlider

A polyglot, event-driven order-processing system built to demonstrate how multiple language runtimes can collaborate over a shared message broker. An HTTP gateway (C#) accepts orders and a processing engine (Scala) maintains a ledger in Postgres — all wired together through RabbitMQ.

```
POST /api/orders
      │
      ▼
 C# Gateway ──► Channel<T> buffer ──► RabbitMqPublisherWorker
                                              │
                                    orders.exchange (topic)
                                         routing key: orders.placed
                                              │
                                             ▼
                                  queue: orders.placed
                                              │
                                        Scala Engine
                              (ledger → Postgres, analytics snapshot)
                                              │
                                       nack → DLX
                                 (dlx.orders.placed)
```

---

## Prerequisites

| Tool | Version | Used by |
|------|---------|---------|
| Docker & Docker Compose | any recent | infrastructure |
| .NET SDK | 9 | C# gateway |
| Java + sbt | 17+ / 1.9+ | Scala engine |
| Python | 3.11+ | load tester, MCP server |

---

## Run

```bash
# Start RabbitMQ and Postgres
docker compose up -d

# Start all services with colour-coded output
./run-all.sh
```

Press `Ctrl+C` to stop everything. The script waits for RabbitMQ and Postgres to be healthy before starting the services.

**Or start services individually** — see each component's README.

---

## Test

```bash
# All suites (requires Docker for the Testcontainers integration test)
./test-all.sh

# Unit tests only — no Docker needed
./test-all.sh --no-integration
```

---

## Contract tests

Cross-service contracts live in `contracts/`:

| File | What it covers |
|------|---------------|
| `contracts/order-api.json` | JSON Schema for `POST /api/orders` request and 202 response |
| `contracts/pacts/scala-engine-cs-gateway.json` | Pact V3 — RabbitMQ message shape agreed between Scala consumer and C# provider |

Run Scala first (generates the pact file), then C#:

```bash
cd processing-engine-scala && sbt test
cd ../gateway-api-cs-tests && dotnet test --filter "Category=Contract"
```

---

## Send an order

```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"ABC123","quantity":2,"customerId":"22222222-2222-4222-8222-222222222222"}'
# → HTTP 202 Accepted  {"message":"Order queued successfully","eventId":"..."}
```

Or fire a batch and get a pass/fail summary:

```bash
./tools/smoke-test.sh        # 20 orders
./tools/smoke-test.sh 50     # custom count
```

---

## Services

| Directory | Language | What it does |
|-----------|----------|--------------|
| [`gateway-api-cs/`](gateway-api-cs/README.md) | C# .NET 9 | Accepts `POST /api/orders`, buffers events, publishes to RabbitMQ |
| [`processing-engine-scala/`](processing-engine-scala/README.md) | Scala 3 | Consumes events, upserts SKU quantities and order counts in Postgres, logs periodic analytics snapshots, routes failures to DLX |
| [`tools/load-tester/`](tools/load-tester/README.md) | Python / Locust | Simulates concurrent users placing orders |
| [`tools/mcp-server/`](tools/mcp-server/README.md) | Python / MCP | Exposes inventory and order tools to AI assistants via the Model Context Protocol |

---

## Event schema

The gateway generates `eventId` and `timestamp`; clients only send the three fields below.

**HTTP request body:**
```json
{ "sku": "string", "quantity": 1, "customerId": "uuid" }
```

**Message published to RabbitMQ** (`orders.exchange`, routing key `orders.placed`):
```json
{
  "eventId": "uuid",
  "sku": "string",
  "quantity": 1,
  "customerId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z"
}
```

---

## Accessing infrastructure

**RabbitMQ management UI** — http://localhost:15672 (credentials: `guest` / `guest`)

**Postgres**
```bash
psql -h localhost -p 5432 -U postgres -d polyglider_inventory
# password: postgres
```

---

## Resiliency

| Risk | Mitigation |
|------|-----------|
| Broker unavailable | Gateway buffers up to 10,000 events in `Channel<T>`; `RabbitMqPublisherWorker` reconnects automatically |
| Consumer failure | Scala engine uses manual ack/nack — message stays on the queue until the DB write succeeds |
| Poison-pill messages | Scala consumer nacks without requeue → message routed to `dlx.orders.placed` for manual triage |
| Duplicate delivery | `processed_events` table guards the ledger; duplicate `eventId` rolls back the transaction |
