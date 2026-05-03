from confluent_kafka import Producer
import json
import os

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka-0.kafka:9092"),
})


def publish_job_created(data):

    print("Publishing job event:", data)

    try:
        producer.produce(
            topic="job-events",
            value=json.dumps(data).encode("utf-8")
        )
        producer.flush()

        print("Successfully sent job event:", data)

    except Exception as e:
        print("Kafka publish error:", str(e))