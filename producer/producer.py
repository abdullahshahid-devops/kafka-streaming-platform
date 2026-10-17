import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "sensor-data")
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "1"))

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            retries=5,
        )
    except Exception as exc:
        print(f"Kafka initialization failed: {exc}; retrying in 5 seconds...", flush=True)
        time.sleep(5)

sensors = [
    ("sensor-01", "warehouse"),
    ("sensor-02", "server-room"),
    ("sensor-03", "office"),
]

try:
    while True:
        sensor_id, location = random.choice(sensors)

        # Occasionally produce a high reading so alert handling can be tested.
        temperature = round(
            random.uniform(27, 35) if random.random() < 0.35
            else random.uniform(18, 29),
            2,
        )

        reading = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "humidity": round(random.uniform(30, 80), 2),
            "location": location,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        metadata = producer.send(
            TOPIC,
            key=sensor_id,
            value=reading,
        ).get(timeout=10)

        print(
            f"Published {reading} "
            f"(partition={metadata.partition}, offset={metadata.offset})",
            flush=True,
        )
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    print("Producer stopping...", flush=True)
finally:
    producer.flush()
    producer.close()
