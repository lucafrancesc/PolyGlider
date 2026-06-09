# Polyglot Event-Driven Fulfillment Ecosystem

## System Architecture Documentation

This document outlines the technical specification, architectural design, and communication patterns of the Polyglot Event-Driven Fulfillment Ecosystem. The system utilizes a decoupled, asynchronous microservices architecture designed for high-throughput transaction ingestion, type-safe processing, and parallel data analytics.

---

## 1. Architectural Overview

The system is designed around the **Publisher-Subscriber (Pub/Sub)** pattern to ensure loose coupling, high availability, and horizontal scalability. By adopting a polyglot approach, each component leverages the unique runtime advantages of its underlying language ecosystem.

### Component Breakdown

1. **Ingestion Gateway (C# .NET):** A high-speed, low-latency HTTP front-door responsible for accepting inbound traffic, performing surface-level schema validation, and publishing events.
2. **Message Broker (RabbitMQ / Apache Kafka):** The centralized event backbone handling asynchronous message distribution, persistence, and backpressure.
3. **Core Processing Engine (Scala 3):** A reactive, functionally pure consumer responsible for executing complex business logic, maintaining ledger consistency, and state mutation.
4. **Analytics & Reporting Worker (Python):** A concurrent consumer focused on data transformation, downstream audit logging, and analytical modeling.

---

## 2. Event Specification & Schema

To maintain contract consistency across disparate language runtimes, events are serialized as standard JSON payloads.

### Event: `OrderPlaced`

* **Routing Key / Topic:** `orders.placed`
* **Content Type:** `application/json`

#### Payload Schema

```json
{
  "eventId": "String (UUIDv4)",
  "sku": "String",
  "quantity": "Integer (Positive)",
  "customerId": "String (UUIDv4)",
  "timestamp": "String (ISO-8601 UTC)"
}

```

---

## 3. Component Deep Dives

### 3.1 Ingestion Gateway (`C# .NET 9`)

* **Framework:** ASP.NET Core Minimal APIs
* **Responsibility:** Receives synchronous `POST /api/orders` requests.
* **Threading Model:** Non-blocking asynchronous I/O utilizing `Task` abstractions.
* **Behavior:** Maps incoming HTTP bodies to a strongly typed record, establishes a connection to the message broker via the native client dependency, publishes the `OrderPlaced` event to the exchange, and immediately returns an HTTP `202 Accepted` status code to the client.

### 3.2 Core Processing Engine (`Scala 3`)

* **Framework:** Cats Effect 3 & FS2 (Functional Streams for Scala)
* **Responsibility:** Implements the deterministic state engine and acts as the system's primary transactional supervisor.
* **Threading Model:** Green-thread concurrency via Cats Effect Fibers.
* **Behavior:** Establishes a persistent consumer connection to the broker. Events are pulled as a reactive, back-pressured stream (`fs2.Stream`). The engine utilizes algebraic data types (ADTs) to strictly enforce domain rules and guarantees type safety during ledger updates, mitigating race conditions at the runtime level.

### 3.3 Analytics Worker (`Python 3.11+`)

* **Framework:** FastAPI / Asyncio & Pika (or aiokafka)
* **Responsibility:** Non-blocking downstream aggregation, data science hooks, and metrics tracking.
* **Threading Model:** Single-threaded event loop utilizing `async/await` syntax.
* **Behavior:** Listens concurrently to the same event stream. Because it is decoupled from the Scala state engine, any processing latency inside the Python runtime (e.g., executing a Pandas transformation or calling an ML model) has zero impact on the primary fulfillment pipeline.

---

## 4. Failure Modes & Resiliency

To prevent data loss in an asynchronous pipeline, the ecosystem implements the following resilience patterns:

| Risk | Mitigation Strategy | Implementation |
| --- | --- | --- |
| **Broker Unavailability** | Local In-Memory Buffering | The C# Gateway implements an internal `System.Threading.Channels` queue to temporarily store events if the connection to the broker drops. |
| **Consumer Failure (Scala/Python)** | Explicit Acknowledgments | Consumers use manual acknowledgment modes (`ack/nack`). An event is only removed from the broker queue *after* it has been successfully committed to downstream stores. |
| **Poison Pill Messages** | Dead Letter Exchange (DLX) | Payloads failing validation or causing unhandled exceptions are routed to a `dlx.orders.placed` queue for isolated manual triage. |

---

## 5. Local Infrastructure Deployment

The ecosystem's backing services are containerized via Docker for local development uniformity.

```yaml
# docker-compose.yml
version: '3.8'

services:
  message-broker:
    image: rabbitmq:3-management-alpine
    container_name: ecosystem_broker
    ports:
      - "5672:5672"   # AMQP protocol port
      - "15672:15672" # Management UI dashboard
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

volumes:
  rabbitmq_data:

---

**Getting Started**

- **Prerequisites:** Docker & Docker Compose, .NET 9 SDK (for gateway), Scala 3 + sbt (for processing engine), Python 3.11+ (for analytics worker).
- **Quick start (local broker):** from the repository root run:

```bash
docker-compose up -d
```

This starts RabbitMQ (management UI on http://localhost:15672, AMQP at `amqp://localhost:5672`).

**Repository Layout**

- `gateway-api-cs/` — C# ingestion gateway (ASP.NET Core)
- `processing-engine-scala/` — Scala core processing engine
- `analytics-worker-python/` — Python analytics worker
 - `tools/load-tester/` — Locust-based load tester and helpers

**Run Locally**

1. Start local infrastructure:

```bash
docker-compose up -d
```

2. Run the gateway (C#):

```bash
cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

3. Run the Scala processing engine (example):

```bash
cd processing-engine-scala
sbt run
```

4. Run the Python analytics worker (example):

```bash
cd analytics-worker-python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python worker.py
```

**Run the apps and tests**

- Start infrastructure:

```bash
docker-compose up -d
```

- Run the gateway (C#):

```bash
cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

- Run the Scala processing engine:

```bash
cd processing-engine-scala
sbt run
```

- Run the Python worker:

```bash
cd analytics-worker-python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python worker.py
```

- Run tests:

```bash
# Scala tests
cd processing-engine-scala && sbt test

# C# gateway tests
dotnet test gateway-api-cs/Tests/gateway-api-cs.Tests.csproj
```

**Example Request**

Send a sample `OrderPlaced` to the gateway:

```bash
curl -X POST http://localhost:5000/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"11111111-1111-4111-8111-111111111111","sku":"ABC123","quantity":1,"customerId":"22222222-2222-4222-8222-222222222222","timestamp":"2026-06-08T12:00:00Z"}'
```

**Development & Contributing**

- Add component-specific build/run instructions in the subfolder READMEs.
- Open a pull request with a clear description and a short verification checklist (build & smoke-test steps).

If you want, I can create starter `README.md` files inside each component folder with exact build and run commands (C#, Scala, Python). Which components should I scaffold first?

```
