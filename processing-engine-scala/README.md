# Processing Engine — Scala 3

The core consumer and ledger. It reads `OrderPlaced` events from RabbitMQ, upserts SKU quantities into a Postgres ledger, and routes failures to a Dead Letter Exchange for manual triage.

**How it works:**
- Declares `orders.placed` queue bound to `orders.exchange` with an `x-dead-letter-exchange` argument pointing to `dlx.orders.exchange`.
- Runs 4 parallel worker fibers (Cats Effect) each pulling from a bounded in-memory queue (capacity 1,000).
- Each message: parse JSON → record `event_id` in `processed_events` + upsert ledger qty in a single transaction → ack. A duplicate `event_id` causes a PK violation that rolls back the ledger write (exactly-once semantics). On any error the message is nacked without requeue and routed to the DLX.
- Flyway runs `V1__create_ledger.sql` and `V2__add_processed_events.sql` at startup when `app.db.runMigrations = true`.

---

## Run

```bash
# Start RabbitMQ and Postgres first
docker compose up -d

cd processing-engine-scala
sbt run
```

### Configuration

**`src/main/resources/application.conf`** — DB settings (already aligned with docker-compose defaults):

| Key | Default |
|-----|---------|
| `app.db.url` | `jdbc:postgresql://localhost:5432/polyglider_inventory` |
| `app.db.user` | `postgres` |
| `app.db.password` | `postgres` |
| `app.db.runMigrations` | `true` |

**RabbitMQ** — read from environment variables (not `application.conf`):

| Variable | Default |
|----------|---------|
| `RABBIT_HOST` | `127.0.0.1` |
| `RABBIT_PORT` | `5672` |
| `RABBIT_USER` | `guest` |
| `RABBIT_PASS` | `guest` |

---

## Test

```bash
cd processing-engine-scala
sbt test
```

Unit tests run against H2 in-memory — no Docker required. They cover JSON parsing, retry logic, UUID validation, ledger upserts, and duplicate event rejection.

`sbt test` also runs the **Pact consumer contract test** (`OrderPlacedEventContractSpec`), which defines the expected `OrderPlaced` message shape and writes the pact file to `contracts/pacts/scala-engine-cs-gateway.json`. The C# gateway's provider tests read this file — run `sbt test` first whenever the message schema changes.
