print("Worker starting... Canary VERSION v2")

from confluent_kafka import Consumer
import json
import os
import time
from database import SessionLocal
from sqlalchemy import text
from prometheus_client import Counter, start_http_server

# Prometheus metric
jobs_processed = Counter("jobs_processed_total", "Total jobs processed")

# Start metrics server
start_http_server(8000)

# Kafka consumer configuration
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka-0.kafka:9092"),
    "group.id": "worker-group-v2",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["job-events"])

print("Worker listening for events...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    job_id = data["job_id"]

    print(f"Received job event {job_id}")

    db = SessionLocal()
    try:
        db.execute(
            text("UPDATE jobs SET status='PROCESSING' WHERE id=:id"),
            {"id": job_id}
        )
        db.commit()

        for _ in range(10**7):
            pass

        db.execute(
            text("UPDATE jobs SET status='COMPLETED' WHERE id=:id"),
            {"id": job_id}
        )
        db.commit()

        print(f"Completed job {job_id}")
        jobs_processed.inc()

    finally:
        db.close()