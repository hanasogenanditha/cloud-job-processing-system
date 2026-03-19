from confluent_kafka import Producer
import json
import os

producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka-0.kafka:9092"),
})

def publish_job_created(job_id):
    print("Publishing job event:", job_id)

    producer.produce(
        "job-events",
        value=json.dumps({"job_id": job_id}).encode("utf-8"),
    )
    producer.flush()
    print("Successfully sent job event:", job_id)