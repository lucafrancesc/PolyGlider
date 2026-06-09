#!/usr/bin/env python3
"""
Send an Order to the PolyGlider gateway API

Usage examples:
  python send_order.py --sku PROD-001 --quantity 3
  python send_order.py --sku PROD-001 --quantity 3 --customer-id 11111111-1111-4111-8111-111111111111 --host localhost --port 5000
"""
import argparse
import json
import sys
import uuid
from typing import Optional

import requests


def send_order(host: str, port: int, sku: str, quantity: int, customer_id: Optional[str]) -> bool:
    if customer_id is None:
        customer_id = str(uuid.uuid4())

    url = f"http://{host}:{port}/api/orders"
    payload = {"sku": sku, "quantity": quantity, "customerId": customer_id}

    try:
        print(f"Sending POST {url}")
        print(json.dumps(payload))
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"Success: {resp.status_code}")
        try:
            print(resp.json())
        except Exception:
            pass
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Send an order to the PolyGlider gateway API")
    parser.add_argument("--sku", required=True, help="Product SKU")
    parser.add_argument("--quantity", type=int, required=True, help="Order quantity")
    parser.add_argument("--customer-id", help="Customer ID (optional; UUID) ")
    parser.add_argument("--host", default="localhost", help="API host (default: localhost)")
    parser.add_argument("--port", type=int, default=5000, help="API port (default: 5000)")
    args = parser.parse_args()

    ok = send_order(args.host, args.port, args.sku, args.quantity, args.customer_id)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
