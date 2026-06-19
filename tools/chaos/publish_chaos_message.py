#!/usr/bin/env python3
"""Publishes directly to orders.exchange, bypassing the C# gateway's input validation.

The gateway rejects malformed input before it ever reaches the broker, so to exercise
the Scala consumer's own failure paths (permanent-failure classification, dedup) these
scenarios have to be injected at the message level instead.

Usage:
    python3 publish_chaos_message.py malformed
    python3 publish_chaos_message.py duplicate [--event-id UUID] [--copies N]
    python3 publish_chaos_message.py transient-in-dlx
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


# Must match RetryPolicy.default.maxRetries in RetryPolicy.scala -- the main consumer's own
# retry budget, not the DLQ reprocessor's separate one.
MAIN_CONSUMER_MAX_RETRIES = 5


def publish(
    channel: "pika.adapters.blocking_connection.BlockingChannel",
    body: bytes,
    properties: "pika.BasicProperties | None" = None,
) -> None:
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=body, properties=properties)


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


def run_transient_in_dlx(channel, sku: str) -> None:
    # Pre-set x-retry-count to the main consumer's own max -- the next transient failure it
    # hits (e.g. Postgres unreachable) will see attemptsSoFar >= maxRetries and nack straight
    # to dlx.orders.placed instead of scheduling another backoff retry. This is the only way to
    # get a *transient*-reason message into dlx.orders.placed without actually waiting out all
    # 5 of the main consumer's real backoff tiers (which take minutes).
    body = order_payload(str(uuid.uuid4()), sku=sku)
    properties = pika.BasicProperties(headers={"x-retry-count": MAIN_CONSUMER_MAX_RETRIES})
    publish(channel, body, properties)
    print(
        f"Published an order pre-tagged with x-retry-count={MAIN_CONSUMER_MAX_RETRIES} "
        "(the main consumer's own max). If Postgres is unreachable when this is consumed, "
        "it nacks straight to dlx.orders.placed with a transient failure reason, instead of "
        "scheduling another retry -- exercising the DLQ reprocessor's own retry-before-escalate "
        "path rather than its immediate-escalate path."
    )


def run_duplicate(channel, event_id: str, copies: int) -> None:
    body = order_payload(event_id)
    for i in range(copies):
        publish(channel, body)
        print(f"Published copy {i + 1}/{copies} with eventId={event_id}")
    print("First copy should process normally; subsequent copies should be an idempotent "
          "no-op (ON CONFLICT DO NOTHING on processed_events -- not a thrown failure), "
          "acking normally without double-counting the ledger. See #78.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="scenario", required=True)
    sub.add_parser("malformed")
    dup = sub.add_parser("duplicate")
    dup.add_argument("--event-id", default=str(uuid.uuid4()))
    dup.add_argument("--copies", type=int, default=2)
    transient = sub.add_parser("transient-in-dlx")
    transient.add_argument("--sku", default="CHAOS-TRANSIENT-DLX")
    args = parser.parse_args()

    connection = pika.BlockingConnection(connection_params())
    try:
        channel = connection.channel()
        if args.scenario == "malformed":
            run_malformed(channel)
        elif args.scenario == "duplicate":
            run_duplicate(channel, args.event_id, args.copies)
        elif args.scenario == "transient-in-dlx":
            run_transient_in_dlx(channel, args.sku)
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
