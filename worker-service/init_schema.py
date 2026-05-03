

from database import engine, Base, init_db
from models import Job, DocumentChunk

def main():
    print("=" * 60)
    print("Initializing Worker Service Database")
    print("=" * 60)
    
    try:
        init_db()
        print("\nDatabase initialization complete!")
        print("pgvector extension created")
        print("All tables created successfully")
        print("  - jobs table")
        print("  - document_chunks table (with pgvector support)")
        print("\nDatabase is ready for use.")
    except Exception as e:
        print(f"\nError during initialization: {e}")
        raise

if __name__ == "__main__":
    main()
