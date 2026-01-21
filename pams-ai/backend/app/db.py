import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:rimthabet@localhost:5432/maxula")
engine = create_engine(DB_URL, pool_pre_ping=True)

def init_db():
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rag_chunks (
            id BIGSERIAL PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT NULL,
            content TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            embedding vector(1024)
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS rag_chunks_vec_idx ON rag_chunks USING ivfflat (embedding vector_cosine_ops);"))

