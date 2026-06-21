import os
import random
import uuid

from locust import HttpUser, task, between

# The gateway's Redis-backed rate limiter keys on client IP -- every Locust user otherwise
# shares one source IP, so a multi-user run immediately collapses onto a single rate-limit
# bucket. Spoofing a distinct X-Forwarded-For per user gives each one its own bucket instead,
# but this only works against nginx in containerized mode: nginx's
# `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for` appends its own address after
# whatever this header already carries (rather than overwriting it), and the gateway is
# configured with a ForwardLimit of 2 specifically so it reads past that appended hop back to
# this spoofed value. Running directly against the gateway (no nginx hop) defeats this --
# there's nothing to append the spoofed value in front of, so the gateway would only ever see
# the single spoofed value (still per-user-distinct, so it still works, just via a different
# mechanism than the nginx case above).
#
# Set LOCUST_SPOOF_SOURCE_IP=0 to disable and run every user from the same (real) IP, e.g. to
# deliberately exercise the rate limiter's blocking behavior under a single bucket.
SPOOF_SOURCE_IP = os.environ.get("LOCUST_SPOOF_SOURCE_IP", "1") != "0"


class PolyGliderLoadTester(HttpUser):
    wait_time = between(0.5, 2.0)

    sku_pool = ["LAPTOP-001", "MOUSE-023", "MONITOR-99", "KEYBOARD-05", "HEADPHONES-12"]

    def on_start(self):
        # One fake IP per simulated user, stable for the user's whole run -- not regenerated
        # per request, since that would make every request from the same user look like a
        # different client instead of giving the user a single consistent bucket.
        self._fake_ip = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    @task
    def place_order(self):
        payload = {
            "sku": random.choice(self.sku_pool),
            "quantity": random.randint(1, 5),
            "customerId": str(uuid.uuid4())
        }
        headers = {"X-Forwarded-For": self._fake_ip} if SPOOF_SOURCE_IP else None
        self.client.post("/api/orders", json=payload, headers=headers)
