import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sensor-data")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "processed-data")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "temp-alerts")
GROUP_ID = os.getenv("GROUP_ID", "processor-group")
THRESHOLD = float(os.getenv("TEMP_ALERT_THRESHOLD", "28"))
ALERT_INTERVAL = int(os.getenv("ALERT_INTERVAL_SECONDS", "60"))

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

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
    except Exception as exc:
        print(f"Kafka initialization failed: {exc}; retrying in 5 seconds...", flush=True)
        time.sleep(5)

windows = defaultdict(lambda: deque(maxlen=10))
last_alert = {}

print(
    f"Processing {INPUT_TOPIC}; threshold={THRESHOLD}°C; "
    f"window=10; group={GROUP_ID}",
    flush=True,
)

try:
    for message in consumer:
        reading = message.value
        sensor_id = reading["sensor_id"]
        temperature = float(reading["temperature"])

        window = windows[sensor_id]
        window.append(temperature)
        rolling_average = sum(window) / len(window)

        enriched = {
            **reading,
            "rolling_avg_temp": round(rolling_average, 2),
            "window_size": len(window),
        }

        producer.send(OUTPUT_TOPIC, key=sensor_id, value=enriched)

        print(
            f"Processed sensor={sensor_id} "
            f"average={rolling_average:.2f}°C window={len(window)}",
            flush=True,
        )

        now = time.time()
        if (
            rolling_average > THRESHOLD
            and now - last_alert.get(sensor_id, 0) >= ALERT_INTERVAL
        ):
            alert = {
                **enriched,
                "alert_type": "HIGH_ROLLING_AVERAGE",
                "threshold": THRESHOLD,
                "alert_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(ALERT_TOPIC, key=sensor_id, value=alert)
            producer.flush()
            last_alert[sensor_id] = now
            print(f"ALERT: {alert}", flush=True)

        producer.flush()
except KeyboardInterrupt:
    print("Processor stopping...", flush=True)
finally:
    consumer.close()
    producer.close()
