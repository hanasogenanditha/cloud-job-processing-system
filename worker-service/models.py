import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON, Text, TypeDecorator
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from database import Base


class Vector(TypeDecorator):
    """Custom type for pgvector"""
    impl = String
    cache_ok = True


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type = Column(String)
    payload = Column(JSON)
    status = Column(String, default="PENDING")
    result = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class DocumentChunk(Base):
    __tablename__ = "document_chunks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), nullable=False)  # References Job.id
    content = Column(Text, nullable=False)
    embedding = Column(Vector, nullable=False)  # pgvector storage
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))