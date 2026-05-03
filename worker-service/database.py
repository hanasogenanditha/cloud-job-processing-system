from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://jobuser:jobpass@localhost:5432/jobdb"
)

print("WORKER DATABASE_URL =", DATABASE_URL)

# Use NullPool to avoid connection pooling issues
engine = create_engine(
    DATABASE_URL,
    poolclass=StaticPool,
    pool_recycle=3600,
    echo=False
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

def init_db():
    """Initialize database with pgvector extension and create tables"""
    try:
        with engine.connect() as conn:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                conn.commit()
                print("✓ pgvector extension initialized")
            except Exception as e:
                print(f"Note: pgvector extension may already exist or error: {e}")
                conn.rollback()
        
        # Drop all existing tables to ensure clean state
        Base.metadata.drop_all(bind=engine)
        print("✓ Dropped existing tables")
        
        # Create all tables from models
        Base.metadata.create_all(bind=engine)
        print("✓ Database tables created successfully")
    except Exception as e:
        print(f"✗ Error during database initialization: {e}")
        raise