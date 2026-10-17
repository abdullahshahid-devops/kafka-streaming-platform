# Containerized Apache Kafka Streaming Platform

A hands-on DevOps and data-streaming project using Docker Compose, Apache
Kafka, ZooKeeper, Python producers and consumers, and Kafka UI.

The platform simulates IoT sensors, streams temperature and humidity readings,
calculates rolling temperature averages, and publishes alerts when configured
thresholds are exceeded.

## Architecture

- **ZooKeeper:** Coordinates the Kafka broker in this Kafka 7.4.0 setup.
- **Kafka:** Stores and distributes sensor and processing events.
- **Python producer:** Generates simulated sensor readings.
- **Python consumer:** Reads sensor events and warns about high temperatures.
- **Stream processor:** Calculates rolling averages using a window of up to
  10 readings per sensor and publishes processed events and alerts.
- **Kafka UI:** Provides a browser interface for inspecting Kafka topics.

## Technology Stack

- Docker Engine
- Docker Compose
- Apache Kafka 7.4.0
- Apache ZooKeeper
- Python
- kafka-python
- kcat
- Kafka UI

## Kafka Topics

| Topic | Purpose |
|---|---|
| `sensor-data` | Raw sensor readings |
| `processed-data` | Readings enriched with rolling averages |
| `temp-alerts` | High rolling-average temperature alerts |
| `ops-events` | Operations event validation |

The project also uses Kafka's internal `__consumer_offsets` topic.

## Prerequisites

- Linux host or virtual machine
- Docker Engine and Docker Compose
- Sufficient memory and disk space for the containers
- Optional: `kcat` for command-line validation

## Start the Platform

Build and start the services:

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
```

Validate the Compose configuration:

```bash
docker compose config --quiet
```

## Verify the Pipeline

View recent application logs:

```bash
docker compose logs --tail=30 producer consumer stream-processor
```

Read processed records from the host:

```bash
kcat -b localhost:29092 -t processed-data -C -o -5 -e -f '%s\n'
```

Read existing temperature alerts:

```bash
kcat -b localhost:29092 -t temp-alerts -C -o beginning -e -f '%s\n'
```

List Kafka metadata:

```bash
kcat -b localhost:29092 -L
```

Inspect consumer-group progress from inside the Kafka container:

```bash
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe --group sensor-group
```

Repeat with `processor-group` to inspect the stream processor's progress.

## Kafka UI

Open the Kafka UI at:

`http://<EC2-HOST-OR-IP>:8080`

Replace the placeholder with the host address you use to access your instance.
Ensure the relevant port is restricted to trusted sources.

## Validation Results

During a verified test run:

- Docker Compose configuration validation succeeded.
- Kafka and ZooKeeper reported healthy status.
- Producer, consumer, and stream processor ran successfully.
- Sensor records were consumed and processed.
- Rolling averages were published to `processed-data`.
- High rolling-average alerts were published to `temp-alerts`.
- Both application consumer groups had zero lag in the captured snapshot.
- Five keyed operations events were published and consumed successfully.

See `pipeline-report.txt` for the captured validation report. Its offset
values represent a point-in-time snapshot and may differ from live values.

## Project Structure

```text
kafka-streaming-lab/
├── consumer/
│   ├── Dockerfile
│   └── consumer.py
├── processor/
│   ├── Dockerfile
│   └── processor.py
├── producer/
│   ├── Dockerfile
│   └── producer.py
├── evidence/
├── docker-compose.yml
├── pipeline-report.txt
└── README.md
```

## Troubleshooting

Check container status:

```bash
docker compose ps
```

Inspect service logs:

```bash
docker compose logs --tail=100 kafka consumer stream-processor
```

Validate Compose syntax:

```bash
docker compose config --quiet
```

Allow time for Kafka to start before running host-side `kcat` commands.
Containers communicate through the internal Docker network using the
Kafka service name and internal listener.

## Security Notes

- This is a learning project, not a production-hardened Kafka deployment.
- The example listeners use PLAINTEXT; do not expose them to untrusted networks.
- Restrict EC2 security-group inbound rules to trusted IP addresses.
- Do not commit credentials, private keys, `.env` files containing secrets,
  virtual environments, or other sensitive data.
- Production deployments should evaluate authentication, TLS, authorization,
  secret management, monitoring, replication, and recovery procedures.

## Learning Outcomes

- Containerized a multi-service streaming application.
- Configured Kafka listeners for internal Docker and host-side clients.
- Worked with Kafka topics, partitions, offsets, and consumer groups.
- Implemented event production, consumption, rolling calculations, and alerts.
- Validated application behavior using logs, `kcat`, and consumer-group offsets.

