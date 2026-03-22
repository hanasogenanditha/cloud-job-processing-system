import requests

url = "http://localhost:8000/jobs"

headers = {
    "Idempotency-Key": "test123"
}

r1 = requests.post(url, headers=headers)
print("First:", r1.json())

r2 = requests.post(url, headers=headers)
print("Second:", r2.json())