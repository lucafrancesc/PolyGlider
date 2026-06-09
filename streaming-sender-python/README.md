# Streaming Sender (Python)

Asynchronous streamer that continuously posts `POST /api/orders` to the gateway for load testing or ingestion simulation.

Install

```bash
python -m pip install -r requirements.txt
```

Run

```bash
python streamer.py --sku TEST-1 --quantity 1 --rate 5 --concurrency 4 --host localhost --port 5000
```

Options
- `--rate`: requests per second
- `--concurrency`: number of concurrent worker coroutines
