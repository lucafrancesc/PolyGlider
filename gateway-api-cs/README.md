# Gateway API — C# .NET 9

The ingestion gateway is the HTTP front-door of the system. It accepts `POST /api/orders` requests, validates the payload, and hands the event off to an in-process channel so the HTTP response is never blocked by broker availability.

**How it works:**
1. The HTTP handler validates `sku` (non-empty) and `quantity` (positive), then writes an `OrderPlacedEvent` to a bounded `Channel<T>` (capacity 10,000) and immediately returns `202 Accepted`.
2. `RabbitMqPublisherWorker` (a `BackgroundService`) drains the channel and publishes events to `orders.exchange` on RabbitMQ with camelCase JSON serialization. If the connection drops it retries every 5 seconds.

---

## Run

```bash
# Start RabbitMQ first
docker compose up -d

cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

Listens on **http://localhost:5187**.

### Environment variables

Configuration is read via `IConfiguration`. Override broker settings with environment variables (double underscore = section separator):

| Variable | Default |
|----------|---------|
| `RABBITMQ__HOST` | `localhost` |
| `RABBITMQ__PORT` | `5672` |
| `RABBITMQ__USER` | `guest` |
| `RABBITMQ__PASSWORD` | `guest` |

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/orders` | Place an order |
| `GET` | `/health` | Returns `{"status":"healthy","bufferAvailable":true/false}` |

**Example:**
```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"LAPTOP-001","quantity":1,"customerId":"22222222-2222-4222-8222-222222222222"}'
```

---

## Test

```bash
# Unit tests — no Docker required
cd gateway-api-cs-tests
dotnet test --filter "Category!=Integration"

# Integration test — spins up a RabbitMQ container via Testcontainers
dotnet test --filter "Category=Integration"

# Both
dotnet test
```
