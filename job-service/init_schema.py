#!/usr/bin/env python3
"""
Database schema initialization script
Run this once to set up pgvector extension and create all tables
"""

from database import engine, Base, init_db
from models import Job

def main():
    print("=" * 60)
    print("Initializing Job Platform Database")
    print("=" * 60)
    
    try:
        init_db()
        print("\n✓ Database initialization complete!")
        print("✓ pgvector extension created")
        print("✓ All tables created successfully")
        print("\nDatabase is ready for use.")
    except Exception as e:
        print(f"\n✗ Error during initialization: {e}")
        raise

if __name__ == "__main__":
    main()
