#!/usr/bin/env python3
"""Asynchronous streaming sender that posts orders to the gateway at a steady rate.

Usage:
  python streamer.py --sku PROD-1 --rate 10 --concurrency 5 --host localhost --port 5000
"""
import argparse
import asyncio
import json
import uuid
from typing import Optional

import aiohttp


async def send_once(session: aiohttp.ClientSession, url: str, sku: str, quantity: int, customer_id: Optional[str]):
    if customer_id is None:
        customer_id = str(uuid.uuid4())
    payload = {"sku": sku, "quantity": quantity, "customerId": customer_id}
    async with session.post(url, json=payload, timeout=10) as resp:
        text = await resp.text()
        return resp.status, text


async def worker(name: int, queue: asyncio.Queue, session: aiohttp.ClientSession):
    while True:
        job = await queue.get()
        if job is None:
            queue.task_done()
            return
        url, sku, qty = job
        try:
            status, text = await send_once(session, url, sku, qty, None)
            print(f"worker-{name}: {status} {text}")
        except Exception as e:
            print(f"worker-{name} error: {e}")
        queue.task_done()


async def produce(url: str, sku: str, quantity: int, rate: float, queue: asyncio.Queue):
    interval = 1.0 / rate if rate > 0 else 0
    while True:
        await queue.put((url, sku, quantity))
        await asyncio.sleep(interval)


async def main():
    parser = argparse.ArgumentParser(description="Streaming sender to gateway API")
    parser.add_argument("--sku", required=True)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--rate", type=float, default=1.0, help="requests per second")
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}/api/orders"
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.concurrency * 10)

    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(worker(i, queue, session)) for i in range(args.concurrency)]
        producer = asyncio.create_task(produce(url, args.sku, args.quantity, args.rate, queue))
        try:
            await asyncio.gather(producer)
        except asyncio.CancelledError:
            pass
        finally:
            for _ in workers:
                await queue.put(None)
            await queue.join()
            for w in workers:
                w.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
