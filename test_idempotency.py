import requests
import uuid

url = "http://127.0.0.1:62536/jobs"

key = str(uuid.uuid4())
headers = {
    "Idempotency-Key": key,
    "Content-Type": "application/json"
}

body = {
    "job_type": "ingest",
    "file_path": "/app/test.pdf"
}

print(f"Using key: {key}")

r1 = requests.post(url, headers=headers, json=body)
print("First:", r1.json())

r2 = requests.post(url, headers=headers, json=body)
print("Second:", r2.json())