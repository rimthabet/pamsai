import os, sys, json, re
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import create_engine, MetaData, Table, select, inspect, text
from sqlalchemy.engine import Engine
from sentence_transformers import SentenceTransformer

from app.db import engine as vec_engine, init_db
from pgvector import Vector
from pgvector.psycopg2 import register_vector


MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")

env:MAXULA_EXCLUDE_TABLES="alembic_version,rag_chunks,rag_sources"



MAXULA_DB_URL = os.getenv(
    "MAXULA_DB_URL",
    "postgresql+psycopg2://postgres:rimthabet@localhost:5432/maxula"
)

# Contrôle: quelles tables indexer
INCLUDE_TABLES = os.getenv("MAXULA_INCLUDE_TABLES", "").strip()
EXCLUDE_TABLES = os.getenv("MAXULA_EXCLUDE_TABLES", "alembic_version").strip()

# Limites
ROW_LIMIT_PER_TABLE = int(os.getenv("MAXULA_ROW_LIMIT", "50000"))
BATCH_SIZE = int(os.getenv("MAXULA_BATCH_SIZE", "128"))

# Filtrage colonnes
EXCLUDE_COLS_REGEX = re.compile(os.getenv("MAXULA_EXCLUDE_COLS_REGEX", r"(password|pwd|secret|token)"), re.I)
MAX_FIELD_CHARS = int(os.getenv("MAXULA_MAX_FIELD_CHARS", "500"))

# Texte final
MAX_DOC_CHARS = int(os.getenv("MAXULA_MAX_DOC_CHARS", "2500"))


def clean_value(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return None
    s = str(v).strip()
    if not s:
        return None
    # tronquer champs trop longs
    if len(s) > MAX_FIELD_CHARS:
        s = s[:MAX_FIELD_CHARS] + "…"
    return s


def row_to_text(table_name: str, row: Dict[str, Any], pk_cols: List[str]) -> str:
    """
    Convertit une ligne en texte générique:
    Table=... PK=... col1=... col2=...
    """
    pk_part = []
    for c in pk_cols:
        cv = clean_value(row.get(c))
        if cv is not None:
            pk_part.append(f"{c}={cv}")
    pk_str = ", ".join(pk_part) if pk_part else "N/A"

    parts = [f"TABLE={table_name}", f"PK={pk_str}"]

    for k, v in row.items():
        if k in pk_cols:
            continue
        if EXCLUDE_COLS_REGEX.search(k or ""):
            continue
        cv = clean_value(v)
        if cv is None:
            continue
        parts.append(f"{k}={cv}")

    text_out = " | ".join(parts)
    if len(text_out) > MAX_DOC_CHARS:
        text_out = text_out[:MAX_DOC_CHARS] + "…"
    return text_out


def get_table_list(engine: Engine) -> List[str]:
    insp = inspect(engine)
    tables = insp.get_table_names()

    inc = [t.strip() for t in INCLUDE_TABLES.split(",") if t.strip()] if INCLUDE_TABLES else []
    exc = set([t.strip() for t in EXCLUDE_TABLES.split(",") if t.strip()] if EXCLUDE_TABLES else [])

    if inc:
        tables = [t for t in tables if t in inc]
    tables = [t for t in tables if t not in exc]
    return tables


def get_pk_columns(engine: Engine, table_name: str) -> List[str]:
    insp = inspect(engine)
    pk = insp.get_pk_constraint(table_name)
    cols = pk.get("constrained_columns") or []
    return cols


def delete_existing(conn_vec, table_name: str):
    # supprime l'index déjà existant pour cette table 
    conn_vec.execute(
        text("DELETE FROM rag_chunks WHERE source_type = :st"),
        {"st": f"maxula:{table_name}"}
    )


def insert_batch(conn_vec, table_name: str, docs: List[Dict[str, Any]], vecs):
    for d, v in zip(docs, vecs):
        conn_vec.execute(
            text("""
                INSERT INTO rag_chunks(source_type, source_id, content, metadata, embedding)
                VALUES (:st, :sid, :content, CAST(:meta AS jsonb), :emb)
            """),
            {
                "st": d["source_type"],
                "sid": d["source_id"],
                "content": d["content"],
                "meta": json.dumps(d["metadata"], ensure_ascii=False),
                "emb": Vector(v.tolist())
            }
        )


def main():
    if not MAXULA_DB_URL:
        raise RuntimeError("MAXULA_DB_URL est vide")

    init_db()

    model = SentenceTransformer(MODEL_NAME)
    maxula_engine = create_engine(MAXULA_DB_URL, pool_pre_ping=True)

    tables = get_table_list(maxula_engine)
    print(f"Tables à indexer: {len(tables)}")
    if not tables:
        print("⚠️ Aucune table trouvée / filtrée.")
        return

    with vec_engine.begin() as conn_vec:
        dbapi_conn = conn_vec.connection.connection
        register_vector(dbapi_conn)

        for tname in tables:
            pk_cols = get_pk_columns(maxula_engine, tname)
            print(f"\nIndexation table: {tname} (PK={pk_cols if pk_cols else 'aucune'})")

            # rebuild complet table
            delete_existing(conn_vec, tname)

            meta = MetaData()
            table = Table(tname, meta, autoload_with=maxula_engine)

            # lecture limitée pour éviter explosion
            stmt = select(table).limit(ROW_LIMIT_PER_TABLE)

            docs_batch = []
            total = 0

            with maxula_engine.begin() as conn_src:
                result = conn_src.execute(stmt).mappings()

                for row in result:
                    row = dict(row)

                    
                    if pk_cols:
                        sid = "|".join([f"{c}={row.get(c)}" for c in pk_cols])
                    else:
                        sid = f"row{total}"

                    content = row_to_text(tname, row, pk_cols)
                    if not content:
                        continue

                    docs_batch.append({
                        "source_type": f"maxula:{tname}",
                        "source_id": sid,
                        "content": content,
                        "metadata": {
                            "db": "maxula",
                            "table": tname,
                            "pk_cols": pk_cols,
                            "pk": {c: row.get(c) for c in pk_cols} if pk_cols else None
                        }
                    })
                    total += 1

                    if len(docs_batch) >= BATCH_SIZE:
                        texts = [d["content"] for d in docs_batch]
                        vecs = model.encode(texts, normalize_embeddings=True)
                        insert_batch(conn_vec, tname, docs_batch, vecs)
                        docs_batch = []

                # flush last
                if docs_batch:
                    texts = [d["content"] for d in docs_batch]
                    vecs = model.encode(texts, normalize_embeddings=True)
                    insert_batch(conn_vec, tname, docs_batch, vecs)

            print(f" Table {tname}: {total} lignes indexées")

    print("\n Indexation auto Maxula terminée.")


if __name__ == "__main__":
    main()
