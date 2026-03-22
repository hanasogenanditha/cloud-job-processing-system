from fastapi import FastAPI, Header, HTTPException
from sqlalchemy.orm import Session
from database import engine, SessionLocal, Base
from models import Job
from kafka_producer import publish_job_created
from redis_client import redis_client
from prometheus_client import Counter, generate_latest
from fastapi.responses import Response
import json

app = FastAPI()

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

jobs_created = Counter("jobs_created_total", "Total jobs created")

IDEMPOTENCY_TTL = 600
LOCK_TTL = 30

@app.post("/jobs")
def create_job(idempotency_key: str = Header(None, alias="Idempotency-Key")):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")

    result_key = f"idemp:result:{idempotency_key}"
    lock_key   = f"idemp:lock:{idempotency_key}"

    # 1. Fast path — already completed
    cached = redis_client.get(result_key)
    if cached:
        return {**json.loads(cached), "message": "Duplicate request"}

    # 2. Acquire distributed lock atomically
    acquired = redis_client.set(lock_key, "1", nx=True, ex=LOCK_TTL)
    if not acquired:
        raise HTTPException(
            status_code=409,
            detail="Concurrent request with the same Idempotency-Key. Retry after 1s."
        )

    try:
        # 3. Double-check after acquiring lock
        cached = redis_client.get(result_key)
        if cached:
            return {**json.loads(cached), "message": "Duplicate request"}

        # 4. Do the work
        jobs_created.inc()
        db: Session = SessionLocal()
        job = Job()
        db.add(job)
        db.commit()
        db.refresh(job)

        publish_job_created(str(job.id))
        db.close()

        # 5. Store result before returning
        result = {"job_id": str(job.id)}
        redis_client.set(result_key, json.dumps(result), ex=IDEMPOTENCY_TTL)

        return {**result, "message": "Job created"}

    finally:
        redis_client.delete(lock_key)


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    cached_status = redis_client.get(job_id)
    if cached_status:
        return {"job_id": job_id, "status": cached_status, "source": "redis"}

    db: Session = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return {"error": "Job not found"}
        redis_client.setex(job_id, 300, job.status)
        return {"job_id": str(job.id), "status": job.status, "source": "database"}
    finally:
        db.close()


@app.get("/redis-test")
def redis_test():
    redis_client.set("test_key", "redis_working")
    value = redis_client.get("test_key")
    return {"redis_value": value}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")