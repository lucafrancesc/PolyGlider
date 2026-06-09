# Analytics Worker (Python)

Simple RabbitMQ consumer that binds to `orders.exchange` with routing key `orders.placed` and prints messages for analysis.

Install

```bash
python -m pip install -r requirements.txt
```

Run

```bash
python consumer.py
```

Configuration
- The script uses `guest:guest` and `localhost:5672` by default. Set environment variables or modify the script for production credentials.
