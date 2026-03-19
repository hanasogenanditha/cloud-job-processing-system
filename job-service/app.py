from fastapi import FastAPI
from sqlalchemy.orm import Session
from database import engine, SessionLocal, Base
from models import Job
from kafka_producer import publish_job_created
from redis_client import redis_client
from prometheus_client import Counter, generate_latest
from fastapi.responses import Response

app = FastAPI()

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

jobs_created = Counter("jobs_created_total", "Total jobs created")

@app.post("/jobs")
def create_job():
    jobs_created.inc()
    db: Session = SessionLocal()
    job = Job()
    db.add(job)
    db.commit()
    db.refresh(job)

    publish_job_created(str(job.id))
    
    db.close()
    return {"job_id": str(job.id), "status": job.status}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):

    cached_status = redis_client.get(job_id)

    if cached_status:
        return {
            "job_id": job_id,
            "status": cached_status,
            "source": "redis"
        }

    db: Session = SessionLocal()

    try:
        job = db.query(Job).filter(Job.id == job_id).first()

        if not job:
            return {"error": "Job not found"}

        redis_client.setex(job_id, 300, job.status)

        return {
            "job_id": str(job.id),
            "status": job.status,
            "source": "database"
        }

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