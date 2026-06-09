# Analytics Client (Python)

Small CLI client to send `POST /api/orders` to the PolyGlider gateway.

Prerequisites
- Python 3.8+
- pip

Install

```bash
python -m pip install -r requirements.txt
```

Usage

```bash
python send_order.py --sku PROD-001 --quantity 2

# customize host/port
python send_order.py --sku PROD-001 --quantity 2 --host localhost --port 5000

# provide your own customer id
python send_order.py --sku PROD-001 --quantity 2 --customer-id 11111111-1111-4111-8111-111111111111
```

The script returns exit code `0` on success and `1` on failure.
