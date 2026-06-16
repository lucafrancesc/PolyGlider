"""
Analytics worker — subscribes to orders.placed events on its own dedicated
queue so it receives every message independently of the Scala processing engine.

Aggregates order count and total quantity per SKU in memory and logs a summary
every SUMMARY_EVERY messages (default 10).

Environment variables (all optional, defaults shown):
  RABBITMQ_HOST      localhost
  RABBITMQ_PORT      5672
  RABBITMQ_USER      guest
  RABBITMQ_PASS      guest
  SUMMARY_EVERY      10
"""

import json
import logging
import os
import time
from collections import defaultdict

import pika
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [analytics] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
SUMMARY_EVERY = int(os.getenv("SUMMARY_EVERY", "10"))

EXCHANGE = "orders.exchange"
QUEUE = "analytics.orders.placed"
ROUTING_KEY = "orders.placed"

# In-memory aggregates (reset on restart — analytics worker is intentionally stateless)
order_count: dict[str, int] = defaultdict(int)
total_qty: dict[str, int] = defaultdict(int)


def log_summary() -> None:
    total_orders = sum(order_count.values())
    total_units = sum(total_qty.values())
    logger.info("── Analytics snapshot (%d orders, %d units) ──", total_orders, total_units)
    for sku in sorted(order_count):
        logger.info("  %-20s  orders=%-6d  units=%d", sku, order_count[sku], total_qty[sku])


def on_message(channel, method, _properties, body) -> None:
    try:
        event = json.loads(body)
        sku: str = event["sku"]
        qty: int = int(event["quantity"])
        order_count[sku] += 1
        total_qty[sku] += qty
        channel.basic_ack(method.delivery_tag)

        if sum(order_count.values()) % SUMMARY_EVERY == 0:
            log_summary()
    except Exception as exc:
        logger.error("Failed to process message: %s | body=%s", exc, body)
        channel.basic_nack(method.delivery_tag, requeue=False)


def connect() -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def main() -> None:
    logger.info("Starting analytics worker (summary every %d messages)", SUMMARY_EVERY)
    while True:
        try:
            conn = connect()
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.queue_declare(queue=QUEUE, durable=True)
            ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)
            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            logger.info("Connected to RabbitMQ at %s:%d — consuming from %s", RABBITMQ_HOST, RABBITMQ_PORT, QUEUE)
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            logger.error("Connection lost (%s), retrying in 5s…", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down")
            break


if __name__ == "__main__":
    main()
