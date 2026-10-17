import json
import os
import time

from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "sensor-data")
GROUP_ID = os.getenv("GROUP_ID", "sensor-group")

consumer = None

while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
    except Exception as exc:
        print(f"Kafka initialization failed: {exc}; retrying in 5 seconds...", flush=True)
        time.sleep(5)

print(f"Listening to topic={TOPIC}, group={GROUP_ID}", flush=True)

try:
    for message in consumer:
        reading = message.value
        temperature = float(reading["temperature"])

        print(
            f"Received sensor={reading['sensor_id']} "
            f"temperature={temperature}°C "
            f"partition={message.partition} offset={message.offset}",
            flush=True,
        )

        if temperature > 30:
            print(
                f"WARNING: High temperature {temperature}°C "
                f"from {reading['sensor_id']} at {reading['location']}",
                flush=True,
            )
except KeyboardInterrupt:
    print("Consumer stopping...", flush=True)
finally:
    consumer.close()
