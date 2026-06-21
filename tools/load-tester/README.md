# Load Tester — Locust

Simulates concurrent users placing orders against the gateway to measure throughput and latency under load.

---

## Run

The gateway's Redis-backed rate limiter caps requests *per client IP* (`GATEWAY__RATELIMITPERMINUTE`,
default 100/min — see the repo root README's [Security](../../README.md#security) section). Every
simulated Locust user otherwise shares one process and therefore one source IP, so a multi-user
run collapses onto a single rate-limit bucket almost immediately. `locustfile.py` works around
this by spoofing a distinct `X-Forwarded-For` per user, giving each one its own bucket — see the
module docstring for why this needed a corresponding `ForwardLimit` change on the gateway side
(`Program.cs`) to actually take effect when run through nginx.

```bash
# Either run mode works — the gateway reads the spoofed per-user IP correctly in both:
docker compose --profile containerized up -d --build --scale gateway=3   # nginx on :80, N replicas
# or: cd gateway-api-cs && dotnet run --project gateway-api-cs.csproj     # host mode, :5187

cd tools/load-tester
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Headless: 100 users, spawn 10/s, run for 1 minute
locust -f locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:80

# Web UI (open http://localhost:8089 to configure and start)
locust -f locustfile.py --host http://localhost:80
```

(Use `--host http://localhost:5187` instead for host mode.) Containerized mode is still the more
realistic target — it's the only one with nginx load-balancing across replicas and a Redis
limiter genuinely shared across them, which is the scenario the rate limiter was built for.

To deliberately exercise rate-limit blocking instead (e.g. to verify 429 behavior), set
`LOCUST_SPOOF_SOURCE_IP=0` so every user shares one real IP:

```bash
LOCUST_SPOOF_SOURCE_IP=0 locust -f locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:80
```

Or override the limit for a run that isn't meant to exercise the limiter at all:

```bash
# In .env, or inline when running the gateway directly:
GATEWAY_RATELIMITPERMINUTE=100000 docker compose --profile containerized up -d --build
```

---

## What it does

Each simulated user picks a random SKU from a fixed pool, a random quantity (1–5), and a random customer UUID, then POSTs to `/api/orders`. Users wait 0.5–2 seconds between requests.

---

## Verify

- Gateway should return `HTTP 202` for every request — Locust reports non-2xx responses as failures.
- RabbitMQ management UI at http://localhost:15672 shows message rates on `orders.placed`.
- Postgres `ledger` table accumulates SKU quantities as the Scala engine processes messages.
- In containerized mode, sustained throughput well above ~1.67 req/s (100/min) with no `429`s confirms per-user IP spoofing is actually reaching the gateway as distinct buckets, not collapsing into one.
