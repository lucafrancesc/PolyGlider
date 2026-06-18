#!/usr/bin/env python3
"""Publishes directly to orders.exchange, bypassing the C# gateway's input validation.

The gateway rejects malformed input before it ever reaches the broker, so to exercise
the Scala consumer's own failure paths (permanent-failure classification, dedup) these
scenarios have to be injected at the message level instead.

Usage:
    python3 publish_chaos_message.py malformed
    python3 publish_chaos_message.py duplicate [--event-id UUID] [--copies N]
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pika

EXCHANGE = "orders.exchange"
ROUTING_KEY = "orders.placed"


def connection_params() -> pika.ConnectionParameters:
    host = os.environ.get("RABBIT_HOST", "127.0.0.1")
    port = int(os.environ.get("RABBIT_PORT", "5672"))
    user = os.environ.get("RABBIT_USER", "guest")
    password = os.environ.get("RABBIT_PASS", "guest")
    return pika.ConnectionParameters(
        host=host, port=port, credentials=pika.PlainCredentials(user, password)
    )


def publish(channel: "pika.adapters.blocking_connection.BlockingChannel", body: bytes) -> None:
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=body)


def order_payload(event_id: str, sku: str = "CHAOS-SKU", quantity: int = 1) -> bytes:
    return json.dumps(
        {
            "eventId": event_id,
            "sku": sku,
            "quantity": quantity,
            "customerId": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ).encode("utf-8")


def run_malformed(channel) -> None:
    # Not valid JSON at all -> circe ParsingFailure -> classified Permanent -> straight to DLX.
    body = b'{"eventId": "not-json-from-here-on...'
    publish(channel, body)
    print("Published malformed (unparseable) payload to orders.exchange")


def run_duplicate(channel, event_id: str, copies: int) -> None:
    body = order_payload(event_id)
    for i in range(copies):
        publish(channel, body)
        print(f"Published copy {i + 1}/{copies} with eventId={event_id}")
    print("First copy should process normally; subsequent copies should hit the "
          "processed_events unique-constraint violation -> classified Permanent -> DLX.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="scenario", required=True)
    sub.add_parser("malformed")
    dup = sub.add_parser("duplicate")
    dup.add_argument("--event-id", default=str(uuid.uuid4()))
    dup.add_argument("--copies", type=int, default=2)
    args = parser.parse_args()

    connection = pika.BlockingConnection(connection_params())
    try:
        channel = connection.channel()
        if args.scenario == "malformed":
            run_malformed(channel)
        elif args.scenario == "duplicate":
            run_duplicate(channel, args.event_id, args.copies)
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
