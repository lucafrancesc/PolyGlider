# Polyglot Event-Driven Fulfillment Ecosystem

## System Architecture Documentation

This document outlines the technical specification, architectural design, and communication patterns of the Polyglot Event-Driven Fulfillment Ecosystem. The system utilizes a decoupled, asynchronous microservices architecture designed for high-throughput transaction ingestion, type-safe processing, and parallel data analytics.

**Sections 1–4 describe the intended (target) design.** See [Implementation status](#implementation-status) for what is built in this repository today, and [Known gaps](#known-gaps) for interoperability issues that still need code changes.

---

## Implementation status

| Component | Status | Notes |
| --- | --- | --- |
| C# ingestion gateway | Implemented | `POST /api/orders`, offline buffer via `Channel<T>`, env-var config |
| RabbitMQ broker | Implemented | via [docker-compose.yml](docker-compose.yml) |
| Scala processing engine | Implemented | manual ack/nack, Postgres ledger, DLX routing on nack |
| Postgres | Implemented in compose | `polyglider_inventory` DB, `runMigrations = true` by default |
| Load tester | Implemented | [tools/load-tester/](tools/load-tester/) |
| Python analytics worker | Planned | directory not present in repo |
| DLX / offline buffer | Implemented | DLX in Scala consumer; `System.Threading.Channels` buffer in C# gateway |
| Kafka | Planned | described in target architecture only |
| C# unit tests | Implemented | `gateway-api-cs-tests/` (5 tests, no Docker required) |
| C# Testcontainers integration tests | Implemented | `gateway-api-cs-tests/` (1 test, requires Docker) |

---

## 1. Architectural Overview (target)

The system is designed around the **Publisher-Subscriber (Pub/Sub)** pattern to ensure loose coupling, high availability, and horizontal scalability. By adopting a polyglot approach, each component leverages the unique runtime advantages of its underlying language ecosystem.

### Component Breakdown

1. **Ingestion Gateway (C# .NET):** A high-speed, low-latency HTTP front-door responsible for accepting inbound traffic, performing surface-level schema validation, and publishing events.
2. **Message Broker (RabbitMQ / Apache Kafka):** The centralized event backbone handling asynchronous message distribution, persistence, and backpressure. *(Kafka is a planned alternative; RabbitMQ is used today.)*
3. **Core Processing Engine (Scala 3):** A reactive, functionally pure consumer responsible for executing complex business logic, maintaining ledger consistency, and state mutation.
4. **Analytics & Reporting Worker (Python):** A concurrent consumer focused on data transformation, downstream audit logging, and analytical modeling. *(Planned — not in repo yet.)*

---

## 2. Event Specification & Schema

To maintain contract consistency across disparate language runtimes, events are serialized as standard JSON payloads.

### Event: `OrderPlaced`

* **Routing Key / Topic:** `orders.placed`
* **Content Type:** `application/json`

#### Canonical payload (on the broker)

The gateway generates `eventId` and `timestamp` server-side and publishes this shape to RabbitMQ:

```json
{
  "eventId": "String (UUIDv4)",
  "sku": "String",
  "quantity": "Integer (Positive)",
  "customerId": "String (UUIDv4)",
  "timestamp": "String (ISO-8601 UTC)"
}
```

#### HTTP request body (gateway input)

Clients send only the fields the gateway needs to construct the event:

```json
{
  "sku": "String",
  "quantity": "Integer (Positive)",
  "customerId": "String (UUIDv4)"
}
```

---

## 3. Component Deep Dives (target)

### 3.1 Ingestion Gateway (`C# .NET 9`)

* **Framework:** ASP.NET Core Minimal APIs
* **Responsibility:** Receives synchronous `POST /api/orders` requests.
* **Threading Model:** Non-blocking asynchronous I/O utilizing `Task` abstractions.
* **Behavior:** Maps incoming HTTP bodies to a strongly typed record, establishes a connection to the message broker via the native client dependency, publishes the `OrderPlaced` event to the exchange, and immediately returns an HTTP `202 Accepted` status code to the client.

### 3.2 Core Processing Engine (`Scala 3`)

* **Framework:** Cats Effect 3 *(FS2 is a dependency; the current consumer uses the Java RabbitMQ client with a Cats Effect bounded queue.)*
* **Responsibility:** Implements the deterministic state engine and acts as the system's primary transactional supervisor.
* **Threading Model:** Green-thread concurrency via Cats Effect Fibers.
* **Behavior:** Establishes a persistent consumer connection to the broker. Events are processed with back-pressure via a bounded queue and worker fibers. The engine utilizes algebraic data types (ADTs) to strictly enforce domain rules and guarantees type safety during ledger updates.

### 3.3 Analytics Worker (`Python 3.11+`) — planned

* **Framework:** FastAPI / Asyncio & Pika (or aiokafka)
* **Responsibility:** Non-blocking downstream aggregation, data science hooks, and metrics tracking.
* **Threading Model:** Single-threaded event loop utilizing `async/await` syntax.
* **Behavior:** Listens concurrently to the same event stream. Because it is decoupled from the Scala state engine, any processing latency inside the Python runtime (e.g., executing a Pandas transformation or calling an ML model) has zero impact on the primary fulfillment pipeline.

---

## 4. Failure Modes & Resiliency (target)

To prevent data loss in an asynchronous pipeline, the ecosystem is designed to implement the following resilience patterns:

| Risk | Mitigation Strategy | Target implementation | Implemented today |
| --- | --- | --- | --- |
| **Broker Unavailability** | Local In-Memory Buffering | C# Gateway uses `System.Threading.Channels` to buffer events if the broker connection drops | Yes — `RabbitMqPublisherWorker` drains `Channel<OrderPlacedEvent>` with auto-reconnect |
| **Consumer Failure (Scala/Python)** | Explicit Acknowledgments | Manual `ack`/`nack`; remove from queue only after downstream commit | Yes (Scala) |
| **Poison Pill Messages** | Dead Letter Exchange (DLX) | Route failures to `dlx.orders.placed` for manual triage | Yes (Scala) — nacked messages routed to `dlx.orders.placed` via `dlx.orders.exchange` |

---

## 5. Local Infrastructure Deployment

The ecosystem's backing services are containerized via Docker for local development uniformity. See [docker-compose.yml](docker-compose.yml) for the authoritative definition. It starts:

* **RabbitMQ** — AMQP on port `5672`, management UI on http://localhost:15672 (default user/pass: `guest`/`guest`)
* **Postgres** — port `5432`, database `polyglider_inventory`, user/pass `postgres`/`postgres`

Quick start from the repository root:

```bash
docker-compose up -d
```

---

## Getting Started

### Prerequisites

* Docker & Docker Compose
* .NET 9 SDK (gateway)
* Java 17+ and sbt (Scala processing engine)
* Python 3.11+ (optional — load tester only; analytics worker is planned)

### Repository layout

* `gateway-api-cs/` — C# ingestion gateway (ASP.NET Core)
* `processing-engine-scala/` — Scala core processing engine
* `tools/load-tester/` — Locust-based load tester
* `analytics-worker-python/` — *(planned)* Python analytics worker

### Run locally

1. Start local infrastructure:

```bash
docker-compose up -d
```

2. Run the gateway (C#):

```bash
cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

By default the gateway listens on **http://localhost:5187** (see `Properties/launchSettings.json`).

3. Run the Scala processing engine:

For ledger persistence, align DB settings with docker-compose before starting. Edit `processing-engine-scala/src/main/resources/application.conf`:

The bundled config defaults to `jdbc:postgresql://localhost:5432/polyglider` with `app.db.runMigrations = false`. To persist ledger data locally, set `runMigrations = true` and ensure the database URL matches a running Postgres instance (create database `polyglider` or change the URL to `polyglider_inventory` from docker-compose).

```bash
cd processing-engine-scala
sbt run
```

4. *(Optional)* Run the load tester — see [tools/load-tester/README.md](tools/load-tester/README.md).

### Tests

```bash
# Scala unit tests (H2 in-memory; no Docker required)
cd processing-engine-scala && sbt test

# C# gateway unit tests (no Docker required)
cd gateway-api-cs-tests && dotnet test --filter "Category!=Integration"

# C# gateway integration tests (requires Docker)
cd gateway-api-cs-tests && dotnet test --filter "Category=Integration"
```

### Example request

Send a sample order to the gateway (gateway generates `eventId` and `timestamp`):

```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"ABC123","quantity":1,"customerId":"22222222-2222-4222-8222-222222222222"}'
```

Expected response: HTTP `202 Accepted` with a generated `eventId`.

---

## Known gaps

1. **Python analytics worker:** Directory `analytics-worker-python/` is not present — the worker is fully planned but not yet implemented.
2. **Kafka:** Described in the target architecture as an alternative broker; RabbitMQ is the only broker used today.
3. **Resiliency — DLX for gateway:** The C# gateway's `RabbitMqPublisherWorker` drops the in-flight message if RabbitMQ goes down mid-publish (at-most-once). The Scala consumer has full DLX routing for failed messages.

---

## Development & Contributing

* Component-specific build and run instructions live in each subfolder README.
* Open a pull request with a clear description and a short verification checklist (build & smoke-test steps).
