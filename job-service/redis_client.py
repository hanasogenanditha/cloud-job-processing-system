from fastapi import FastAPI, Header, HTTPException, Depends
from sqlalchemy.orm import Session
from confluent_kafka import Producer
import redis
import json
import time

from database import SessionLocal
from models import Job

app = FastAPI()

redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

producer = Producer({
    "bootstrap.servers": "kafka-0.kafka:9092"
})

IDEMPOTENCY_TTL = 600
LOCK_TTL = 30  

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/jobs")
def create_job(
    idempotency_key: str = Header(None, alias="Idempotency-Key"),
    db: Session = Depends(get_db)
):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")

    result_key = f"idemp:result:{idempotency_key}"
    lock_key   = f"idemp:lock:{idempotency_key}"

    cached = redis_client.get(result_key)
    if cached:
        return {**json.loads(cached), "message": "Duplicate request"}

    acquired = redis_client.set(lock_key, "1", nx=True, ex=LOCK_TTL)
    if not acquired:
        retry
        raise HTTPException(
            status_code=409,
            detail="Concurrent request with the same Idempotency-Key. Retry after 1s."
        )

    try: 
        cached = redis_client.get(result_key)
        if cached:
            return {**json.loads(cached), "message": "Duplicate request"}

        job = Job(status="PENDING")
        db.add(job)
        db.commit()
        db.refresh(job)

        producer.produce(
            topic="job-events",
            value=json.dumps({"job_id": job.id})
        )
        producer.flush()

        result = {"job_id": job.id}
        redis_client.set(result_key, json.dumps(result), ex=IDEMPOTENCY_TTL)

        return {**result, "message": "Job created"}

    finally:
        redis_client.delete(lock_key)


@app.get("/jobs/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.id,
        "status": job.status
    }