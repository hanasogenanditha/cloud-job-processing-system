# Cloud Job Processing System with RAG Pipeline

![CI-CD Pipeline](https://github.com/hanasogenanditha/cloud-job-processing-system/actions/workflows/main.yml/badge.svg)

A production-grade, cloud-native, event-driven job processing system extended with a **Retrieval Augmented Generation (RAG)** pipeline — all deployed on Kubernetes. Upload PDFs, store them as vector embeddings, and query documents using natural language answered by an LLM grounded in your content.

---

## Architecture

```
Client
  │
  ├── POST /upload  ──────────────────────────────► PersistentVolume (/app/pdfs)
  │                                                        │
  └── POST /jobs ─► FastAPI (job-service)                  │
                         │                                 │
                         ├── Redis (idempotency lock)      │
                         │                                 │
                         └── Kafka (job-events topic)      │
                                    │                      │
                                    ▼                      │
                            Worker Service ◄───────────────┘
                            │             │
                            ▼             ▼
                      PDF Extract      LLM Query
                      (PyMuPDF)     (Gemini 2.5 Flash)
                            │             │
                            ▼             ▼
                        pgvector      Job Result
                        (embeddings)      │
                                         ▼
                                    PostgreSQL
                                         │
                                         ▼
                              GET /jobs/{id} ─► Client
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI |
| Message Queue | Apache Kafka + Zookeeper |
| Cache / Locking | Redis |
| Database | PostgreSQL + pgvector |
| Embeddings | Gemini Embedding API (3072 dimensions) |
| LLM | Gemini 2.5 Flash |
| PDF Parsing | PyMuPDF |
| Containerization | Docker |
| Orchestration | Kubernetes (Minikube) |
| Monitoring | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Storage | PersistentVolumeClaim |

---

## Features

- **Event-driven architecture** — jobs are queued via Kafka and processed asynchronously
- **RAG pipeline** — LLM answers are grounded in retrieved document chunks, not hallucinated
- **Vector search** — semantic similarity search using pgvector's `<->` operator
- **Idempotency** — duplicate requests are safely deduplicated using Redis distributed locks
- **PDF persistence** — uploaded PDFs survive pod restarts via PVC
- **Auto-scaling** — HPA scales worker pods from 1 to 5 replicas based on CPU
- **Canary deployments** — safe releases with traffic splitting
- **Prometheus + Grafana metrics** — `jobs_created_total`, `jobs_processed_total`, `jobs_failed_total`
- **CI/CD pipeline** — automated Docker image builds and pushes to Docker Hub on every push to `main`

---

## Project Structure

```
job-platform/
├── job-service/          # FastAPI REST API
│   ├── app.py            # Routes: /upload, /jobs, /jobs/{id}, /metrics
│   ├── models.py         # SQLAlchemy Job model
│   ├── database.py       # DB connection + init
│   ├── kafka_producer.py # Publishes job events
│   ├── redis_client.py   # Redis connection
│   └── Dockerfile
├── worker-service/       # Kafka consumer + RAG processor
│   ├── worker.py         # Ingest + query pipeline
│   ├── database.py
│   └── Dockerfile
├── k8s/                  # Kubernetes manifests
│   ├── job-service.yaml
│   ├── worker-service.yaml
│   ├── worker-pvc.yaml
│   ├── postgres.yaml
│   ├── redis.yaml
│   ├── kafka.yaml
│   ├── kafka-statefulset.yaml
│   ├── zookeeper.yaml
│   ├── HPA.yaml
│   ├── worker-canary.yaml
│   ├── job-metrics-service.yaml
│   └── worker-metrics-service.yaml
└── .github/
    └── workflows/        # CI/CD pipeline
```

---

## Getting Started

### Prerequisites

- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Docker](https://www.docker.com/)
- [A Gemini API key](https://aistudio.google.com/app/apikey)

### 1. Clone the repo

```bash
git clone https://github.com/hanasogenanditha/cloud-job-processing-system
cd job-platform
```

### 2. Start Minikube

```bash
minikube start
minikube docker-env | Invoke-Expression
```

### 3. Build Docker images

```bash
docker build -t job-service:latest ./job-service
docker build -t worker-service:latest ./worker-service
```

### 4. Deploy all services

```bash
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-statefulset.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/worker-pvc.yaml
kubectl apply -f k8s/worker-service.yaml
kubectl apply -f k8s/job-service.yaml
kubectl apply -f k8s/HPA.yaml
```

### 5. Verify all pods are running

```bash
kubectl get pods
```

Expected output:
```
NAME                             READY   STATUS    RESTARTS
job-service-xxx                  1/1     Running   0
kafka-0                          1/1     Running   0
postgres-xxx                     1/1     Running   0
redis-xxx                        1/1     Running   0
worker-service-xxx               1/1     Running   0
zookeeper-xxx                    1/1     Running   0
```

### 6. Forward the API port

```bash
kubectl port-forward svc/job-service 8080:8000
```

API available at: `http://localhost:8080/docs`

---

## API Usage

### Upload a PDF

```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@/path/to/document.pdf"
```

Response:
```json
{
  "filename": "document.pdf",
  "file_path": "/app/pdfs/document.pdf"
}
```

### Ingest the PDF (extract + embed + store)

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: <unique-uuid>" \
  -d '{
    "job_type": "ingest",
    "payload": {
      "file_path": "/app/pdfs/document.pdf"
    }
  }'
```

Response:
```json
{
  "job_id": "55d56be1-4f6a-486c-b6ab-33c53bdc0bf9",
  "message": "Job created"
}
```

### Query the document

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: <unique-uuid>" \
  -d '{
    "job_type": "query",
    "payload": {
      "question": "What are the main topics covered in this document?"
    }
  }'
```

### Check job status and result

```bash
curl http://localhost:8080/jobs/<job_id>
```

Response:
```json
{
  "job_id": "49379e2b-09f8-4a4b-827f-f217365c7fbe",
  "status": "COMPLETED",
  "result": "The document covers...",
  "source": "database"
}
```

Job statuses: `PENDING` → `PROCESSING` → `COMPLETED` / `FAILED`

---

## How It Works

### Job Processing Flow
1. Client sends `POST /jobs` with an `Idempotency-Key` header
2. Job-service checks Redis — if key exists, returns cached `job_id` (duplicate request)
3. If new, job is stored in PostgreSQL with status `PENDING`
4. Job event is published to Kafka (`job-events` topic)
5. Worker service consumes the event from Kafka
6. Worker updates job status to `PROCESSING`
7. Based on `job_type`, worker routes to either the **ingest pipeline** or **query pipeline**
8. On completion, job status is updated to `COMPLETED` and result stored in PostgreSQL
9. Client polls `GET /jobs/{id}` to retrieve the result

### Ingest Flow
1. PDF is uploaded via `POST /upload` and saved to the persistent volume
2. An ingest job is created and published to Kafka
3. The worker consumes the event and extracts text using PyMuPDF
4. Text is split into 500-word chunks
5. Each chunk is embedded using the Gemini Embedding API (3072 dimensions)
6. Embeddings are stored in pgvector

### Query Flow
1. A query job is created and published to Kafka
2. The worker embeds the question using the same embedding model
3. Semantic similarity search retrieves the top 3 most relevant chunks via pgvector
4. The chunks + question are sent to Gemini 2.5 Flash as context
5. The LLM answer is stored in PostgreSQL and returned via `GET /jobs/{id}`

---

## Idempotency

Every `POST /jobs` request requires an `Idempotency-Key` header (a UUID). If the same key is sent twice, the second request returns the cached result instead of creating a duplicate job.

```bash
# Safe to retry — won't create duplicate jobs
curl -X POST http://localhost:8080/jobs \
  -H "Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000" \
  ...
```

Testing idempotency:
```python
import requests
url = "http://localhost:8080/jobs"
headers = {"Idempotency-Key": "test123"}
print(requests.post(url, headers=headers).json())
print(requests.post(url, headers=headers).json())
```

Output:
```
First:  {'job_id': '95b48af3-327b-4866-82ca-d0969ad6ffc0', 'message': 'Job created'}
Second: {'job_id': '95b48af3-327b-4866-82ca-d0969ad6ffc0', 'message': 'Duplicate request'}
```

---

## Monitoring

Prometheus metrics are exposed at `/metrics` on both services and visualized via Grafana dashboards:

| Metric | Description |
|---|---|
| `jobs_created_total` | Total jobs created by job-service |
| `jobs_processed_total` | Total jobs successfully processed by worker |
| `jobs_failed_total` | Total jobs that failed in worker |

---

## Autoscaling

- Horizontal Pod Autoscaler (HPA) based on CPU usage
- Min: 1 pod, Max: 5 pods

---

## CI/CD Pipeline

GitHub Actions automatically:
- Triggers on every push to `main`
- Builds Docker images
- Pushes to Docker Hub
- Deploys updated version

---

## Canary Deployment

- Deploy new version alongside old
- Split traffic between versions
- Validate before full rollout — zero downtime releases

---

## Future Improvements

- Retry mechanism with dead-letter queues (DLQ) for Kafka failures
- Full JWT enforcement at API Gateway
- Resume parsing with PDF support for ATS-style use cases
- Load testing and performance tuning
- Enhanced fault tolerance
- Elasticsearch integration for hybrid search

---

## Author

[hanasogenanditha](https://github.com/hanasogenanditha)
