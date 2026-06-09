#!/usr/bin/env python3
"""Simple RabbitMQ consumer that listens to orders.placed and prints payloads for analysis.
Requires a running RabbitMQ instance and the exchange 'orders.exchange' declared by the gateway.
"""
import json
import sys
import time

import pika


def on_message(ch, method, properties, body):
    try:
        payload = json.loads(body)
    except Exception:
        payload = body.decode(errors='replace')
    print("[MSG]", method.routing_key, payload)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(host: str = "localhost", port: int = 5672, user: str = "guest", password: str = "guest"):
    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(host=host, port=port, credentials=credentials)

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    exchange = 'orders.exchange'
    ch.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)

    result = ch.queue_declare('', exclusive=True)
    queue_name = result.method.queue
    ch.queue_bind(exchange=exchange, queue=queue_name, routing_key='orders.placed')

    print("[*] Waiting for messages. To exit press CTRL+C")
    ch.basic_consume(queue=queue_name, on_message_callback=on_message)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            ch.stop_consuming()
        except Exception:
            pass
    finally:
        conn.close()


if __name__ == '__main__':
    main()
