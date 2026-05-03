print("Worker starting... Canary VERSION v4")

from confluent_kafka import Consumer
import json
import os
import uuid
from database import SessionLocal
from sqlalchemy import text
from prometheus_client import Counter, start_http_server

import fitz
from google import genai
client_genai = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

jobs_processed = Counter("jobs_processed_total", "Total jobs processed")
jobs_failed = Counter("jobs_failed_total", "Total jobs failed")

start_http_server(8001)

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka-0.kafka:9092"),
    "group.id": "worker-group-v6",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["job-events"])

print("Worker listening for events...")

PDF_DIR = "/app/pdfs"


# ---------------- HELPERS ----------------

def extract_text_from_pdf(file_path):
    print("Opening PDF:", file_path)
    doc = fitz.open(file_path)
    text_data = ""
    for page in doc:
        text_data += page.get_text()
    print("Extracted text length:", len(text_data))
    return text_data

def split_text(text, chunk_size=500):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
    print("Chunks created:", len(chunks))
    return chunks

def get_embedding(text):
    result = client_genai.models.embed_content(
        model="models/gemini-embedding-001",
        contents=text
    )
    return result.embeddings[0].values

def ask_llm(question, chunks):
    context = "\n".join(chunks)
    prompt = f"""
    Answer the question using ONLY the context below.
    Context:
    {context}
    Question:
    {question}
    """
    response = client_genai.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    return response.text


# ---------------- MAIN LOOP ----------------

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))

    job_id = data["job_id"]
    job_type = data.get("job_type")
    payload = data.get("payload", {})

    # Handle double-nested payload from older messages
    if "payload" in payload and isinstance(payload.get("payload"), dict):
        payload = payload["payload"]

    print(f"\n===== NEW JOB =====")
    print("Job ID:", job_id)
    print("Job Type:", job_type)
    print("Payload:", payload)

    db = SessionLocal()
    try:
        db.execute(
            text("UPDATE jobs SET status='PROCESSING' WHERE id=:id"),
            {"id": job_id}
        )
        db.commit()

        # ---------------- INGEST ----------------
        if job_type == "ingest":
            print("INGEST FLOW STARTED")

            file_path = payload.get("file_path")

            # Fall back to PDF_DIR if only a filename is given or no path provided
            if file_path and not os.path.isabs(file_path):
                file_path = os.path.join(PDF_DIR, file_path)
            elif not file_path:
                file_path = os.path.join(PDF_DIR, "sample.pdf")

            print("Resolved file path:", file_path)

            if not os.path.exists(file_path):
                print("ERROR: file does NOT exist at path:", file_path)
                db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
                db.commit()
                jobs_failed.inc()

            else:
                try:
                    text_data = extract_text_from_pdf(file_path)

                    if not text_data.strip():
                        print("WARNING: Extracted text is empty")

                    chunks = split_text(text_data)
                    inserted = 0

                    for chunk in chunks:
                        if chunk.strip():
                            embedding = get_embedding(chunk)
                            chunk_id = str(uuid.uuid4())

                            db.execute(
                                text("""
                                    INSERT INTO document_chunks (id, document_id, content, embedding)
                                    VALUES (:id, :doc_id, :content, :embedding)
                                """),
                                {
                                    "id": chunk_id,
                                    "doc_id": job_id,
                                    "content": chunk,
                                    "embedding": str(embedding)
                                }
                            )
                            inserted += 1

                    db.commit()
                    print(f"Inserted {inserted} chunks into DB")

                    db.execute(
                        text("UPDATE jobs SET status='COMPLETED' WHERE id=:id"),
                        {"id": job_id}
                    )
                    db.commit()
                    print(f"Completed job {job_id}")
                    jobs_processed.inc()

                except Exception as e:
                    print("ERROR during ingest:", str(e))
                    db.rollback()
                    db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
                    db.commit()
                    jobs_failed.inc()

        # ---------------- QUERY ----------------
        elif job_type == "query":
            print("QUERY FLOW STARTED")

            question = payload.get("question")
            print("Question:", question)

            if not question:
                print("ERROR: question is missing")
                db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
                db.commit()
                jobs_failed.inc()

            else:
                try:
                    question_embedding = get_embedding(question)

                    result = db.execute(
                        text("""
                            SELECT content
                            FROM document_chunks
                            ORDER BY embedding <-> :embedding
                            LIMIT 3
                        """),
                        {"embedding": str(question_embedding)}
                    )

                    chunks = [row[0] for row in result.fetchall()]
                    print("Retrieved chunks:", len(chunks))

                    if not chunks:
                        print("WARNING: No relevant chunks found")

                    answer = ask_llm(question, chunks)

                    db.execute(
                        text("UPDATE jobs SET result=:result, status='COMPLETED' WHERE id=:id"),
                        {"result": answer, "id": job_id}
                    )
                    db.commit()
                    print(f"Completed job {job_id}")
                    jobs_processed.inc()

                except Exception as e:
                        print("ERROR during query:", str(e))
                        db.rollback()
                        db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
                        db.commit()
                        jobs_failed.inc()

        else:
            print(f"Unknown job_type: {job_type}")
            db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
            db.commit()
            jobs_failed.inc()

    except Exception as e:
        print("FATAL ERROR processing job:", str(e))
        db.rollback()
        db.execute(text("UPDATE jobs SET status='FAILED' WHERE id=:id"), {"id": job_id})
        db.commit()
        jobs_failed.inc()

    finally:
        db.close()