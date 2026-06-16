# Load Tester — Locust

Simulates concurrent users placing orders against the gateway to measure throughput and latency under load.

---

## Run

```bash
# Start infrastructure and the gateway first
docker compose up -d
cd gateway-api-cs && dotnet run --project gateway-api-cs.csproj

# Set up the load tester
cd tools/load-tester
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Headless: 100 users, spawn 10/s, run for 1 minute
locust -f locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:5187

# Web UI (open http://localhost:8089 to configure and start)
locust -f locustfile.py --host http://localhost:5187
```

---

## What it does

Each simulated user picks a random SKU from a fixed pool, a random quantity (1–5), and a random customer UUID, then POSTs to `/api/orders`. Users wait 0.5–2 seconds between requests.

---

## Verify

- Gateway should return `HTTP 202` for every request — Locust reports non-2xx responses as failures.
- RabbitMQ management UI at http://localhost:15672 shows message rates on `orders.placed`.
- Postgres `ledger` table accumulates SKU quantities as the Scala engine processes messages.
