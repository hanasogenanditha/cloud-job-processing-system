# Event-Driven Job Processing System

![CI-CD Pipeline](https://github.com/hanasogenanditha/cloud-job-processing-system/actions/workflows/main.yml/badge.svg)

A cloud-native, scalable job processing system built using microservices architecture. This system enables asynchronous job execution using Kafka and is deployed on Kubernetes with monitoring, autoscaling, and CI/CD.

---

## Architecture
```
Client
  ↓
FastAPI Job Service
  ↓
PostgreSQL
  ↓
Kafka (job-events)
  ↓
Worker Service
  ↓
PostgreSQL
```

---

## Tech Stack

- **Backend:** FastAPI
- **Messaging:** Apache Kafka (confluent_kafka)
- **Database:** PostgreSQL
- **Cache / Idempotency:** Redis
- **Containerization:** Docker
- **Orchestration:** Kubernetes (Minikube)
- **Monitoring:** Prometheus + Grafana
- **CI/CD:** GitHub Actions

---

## Features

- Event-driven architecture using Kafka
- Asynchronous job processing via worker service
- PostgreSQL-based persistent storage
- Redis-based idempotency to prevent duplicate jobs
- Kubernetes deployment with autoscaling (HPA)
- Prometheus metrics and Grafana dashboards
- CI/CD pipeline with Docker image builds
- Canary deployment for safe releases

---

## Job Flow

1. Client sends request: `POST /jobs`
2. Job stored in PostgreSQL (status = `PENDING`)
3. Event published to Kafka
4. Worker consumes event
5. Worker updates job status: `PROCESSING` → `COMPLETED`

---

## Idempotency (Redis)

To prevent duplicate job creation:

- Client sends header:
```
Idempotency-Key: <unique-key>
```

- Redis stores:
```
idemp:result:<key> → job_id
```

- Duplicate requests return the same `job_id`

---

## Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/hanasogenanditha/cloud-job-processing-system
cd job-platform
```

### 2. Start Minikube and point Docker to it
```bash
minikube start
minikube docker-env | Invoke-Expression
```

### 3. Build Docker images
```bash
docker build -t job-service:latest ./job-service
docker build -t worker-service:latest ./worker-service
```

### 4. Deploy to Kubernetes
```bash
kubectl apply -f k8s/
```

### 5. Access service
```bash
kubectl port-forward service/job-service 8000:8000
```

API available at: `http://localhost:8000/docs`

---

## Testing Idempotency
```python
import requests

url = "http://localhost:8000/jobs"
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

- Prometheus scrapes `/metrics` endpoint
- Grafana dashboards visualize:
  - `jobs_created_total`
  - `jobs_processed_total`

---

## Autoscaling

- Horizontal Pod Autoscaler (HPA) based on CPU usage
- Min: 1 pod, Max: 5 pods

---

## CI/CD Pipeline

- Triggered on code push
- Builds Docker image
- Pushes to Docker Hub
- Deploys updated version

---

## Canary Deployment

- Deploy new version alongside old
- Split traffic between versions
- Validate before full rollout

---

## Key Learnings

- Event-driven microservices design
- Kubernetes deployment & scaling
- Debugging containerized systems
- Implementing idempotent APIs
- Observability with Prometheus & Grafana

---

## Future Improvements

- Retry mechanism with dead-letter queues
- Job result retrieval API
- Load testing & performance tuning
- Enhanced fault tolerance

---

## Author

[hanasogenanditha](https://github.com/hanasogenanditha)