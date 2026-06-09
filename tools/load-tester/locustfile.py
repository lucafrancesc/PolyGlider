import random
import uuid

try:
    from locust import HttpUser, task, between
except Exception:
    # Provide lightweight fallbacks when locust is not available (e.g., for linting or static analysis)
    def task(func=None):
        if func is None:
            return lambda f: f
        return func

    def between(a, b):
        # return a simple callable similar to locust.between
        return lambda _: (a + b) / 2.0

    class HttpUser:
        wait_time = None
        class _Client:
            @staticmethod
            def post(*args, **kwargs):
                return None
        client = _Client()

class PolyGliderLoadTester(HttpUser):
    # Simulates a user waiting between 0.5 and 2 seconds between placing orders
    wait_time = between(0.5, 2.0)

    # A pool of static SKUs to simulate real inventory item distributions
    sku_pool = ["LAPTOP-001", "MOUSE-023", "MONITOR-99", "KEYBOARD-05", "HEADPHONES-12"]

    @task
    def place_order(self):
        """
        Simulates a user hitting the C# Ingestion Gateway to place an order
        """
        payload = {
            "sku": random.choice(self.sku_pool),
            "quantity": random.randint(1, 5),
            "customerId": str(uuid.uuid4())
        }

        headers = {
            "Content-Type": "application/json"
        }

        # Fires a POST request to the C# API gateway
        self.client.post("/api/orders", json=payload, headers=headers)
