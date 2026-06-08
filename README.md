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

```
