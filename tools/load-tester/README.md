# Locust Load Tester

This README explains how to run the Locust load test located at `tools/load-tester/locustfile.py` against the ingestion gateway in this repository.

Prerequisites:

- Python 3.8+ (3.11 recommended)
- `pip` and virtualenv or `python -m venv`
- Locust (`locust` package)
- Local infrastructure running (RabbitMQ) and the ingestion gateway (C#) running locally

Quick-start (recommended development flow):

1. Start local infrastructure and the gateway from the repository root:

```bash
docker-compose up -d
cd gateway-api-cs
dotnet run --project gateway-api-cs.csproj
```

2. Set up a Python virtual environment and install Locust:

```bash
cd tools/load-tester
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Run Locust in headless mode (example):

```bash
# single-worker headless run: 100 users, spawn rate 10 users/s, run for 1m
locust -f locustfile.py --headless -u 100 -r 10 --run-time 1m --host http://localhost:5187
```

4. Or run the web UI:

```bash
locust -f locustfile.py --host http://localhost:5187
```

Then open http://localhost:8089 in your browser and start the test from the UI.

Configuration notes:

- `--host` should point to the gateway base URL (default in examples: `http://localhost:5187`).
- `locustfile.py` POSTs to `/api/orders` with `sku`, `quantity`, and `customerId` (gateway input shape).
- Adjust user count (`-u`) and spawn rate (`-r`) to match your environment.

Verification:

- The gateway should respond with HTTP `202 Accepted` for valid payloads.
- Monitor RabbitMQ Management UI at http://localhost:15672 to confirm messages are published.

Troubleshooting:

- If Locust reports connection errors, ensure the gateway is listening and `--host` is correct.
- If messages are not reaching RabbitMQ, check gateway logs and the broker connectivity settings in `gateway-api-cs`.
