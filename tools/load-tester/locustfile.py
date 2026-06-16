import random
import uuid

from locust import HttpUser, task, between


class PolyGliderLoadTester(HttpUser):
    wait_time = between(0.5, 2.0)

    sku_pool = ["LAPTOP-001", "MOUSE-023", "MONITOR-99", "KEYBOARD-05", "HEADPHONES-12"]

    @task
    def place_order(self):
        payload = {
            "sku": random.choice(self.sku_pool),
            "quantity": random.randint(1, 5),
            "customerId": str(uuid.uuid4())
        }
        self.client.post("/api/orders", json=payload)
