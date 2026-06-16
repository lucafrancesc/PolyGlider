# Analytics Worker — Python 3.11

A lightweight consumer that tracks order volume and units per SKU in real time. It binds its own dedicated queue (`analytics.orders.placed`) to the same exchange as the Scala engine, so it receives every event independently without competing with the ledger consumer.

**How it works:**
- Connects to RabbitMQ and declares `analytics.orders.placed` (durable) bound to `orders.exchange` with routing key `orders.placed`.
- For each message: parses the JSON payload, increments in-memory counters for `order_count[sku]` and `total_qty[sku]`, then acks. Logs a per-SKU snapshot every `SUMMARY_EVERY` messages (default 10).
- Reconnects automatically with a 5-second backoff if the broker connection drops.
- Aggregates are in-memory only — they reset on restart.

---

## Run

```bash
# Start RabbitMQ first
docker compose up -d

cd analytics-worker-python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Or via Docker Compose (opt-in profile):

```bash
docker compose --profile analytics up
```

### Environment variables

Copy `.env.example` to `.env` to override defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_HOST` | `localhost` | RabbitMQ hostname |
| `RABBITMQ_PORT` | `5672` | AMQP port |
| `RABBITMQ_USER` | `guest` | Username |
| `RABBITMQ_PASS` | `guest` | Password |
| `SUMMARY_EVERY` | `10` | Log a snapshot every N messages |

---

## Verify

Send some orders via the gateway and watch the worker output:

```bash
./tools/smoke-test.sh 20
```

After 10 messages you should see something like:

```
09:15:00 [analytics] INFO ── Analytics snapshot (10 orders, 27 units) ──
09:15:00 [analytics] INFO   KEYBOARD-05           orders=2       units=5
09:15:00 [analytics] INFO   LAPTOP-001            orders=3       units=9
09:15:00 [analytics] INFO   MOUSE-023             orders=5       units=13
```
