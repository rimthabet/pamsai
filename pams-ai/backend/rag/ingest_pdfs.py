import sys, os, glob, json, re, hashlib
from typing import List, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pdf2image import convert_from_path
from PIL import ImageOps
import pytesseract
from sentence_transformers import SentenceTransformer
from sqlalchemy import text
from app.db import engine, init_db

from pgvector import Vector
from pgvector.psycopg2 import register_vector

# -----------------------------
# CONFIG 
# -----------------------------
pytesseract.pytesseract.tesseract_cmd = r"C:\Users\rthabet\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
os.environ["TESSDATA_PREFIX"] = r"C:\Users\rthabet\AppData\Local\Programs\Tesseract-OCR\tessdata"

MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")

# OCR
OCR_LANG = "fra+eng"
PDF_DPI = 400
TESS_CONFIG = "--oem 1 --psm 6"

# Chunking
MAX_CHARS = 1400
OVERLAP = 200

# Qualité chunks
MIN_CHUNK_CHARS = 250
MIN_ALPHA_RATIO = 0.55


# -----------------------------
# RAG SOURCES TABLE 
# -----------------------------
def ensure_rag_sources(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rag_sources (
          id bigserial PRIMARY KEY,
          source_type text NOT NULL,
          source_id text NOT NULL,
          checksum text,
          updated_at timestamptz,
          indexed_at timestamptz NOT NULL DEFAULT now(),
          meta jsonb NOT NULL DEFAULT '{}'::jsonb,
          UNIQUE (source_type, source_id)
        );
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rag_sources_updated_at ON rag_sources(updated_at);"))


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def already_indexed_same_checksum(conn, source_type: str, source_id: str, checksum: str) -> bool:
    row = conn.execute(
        text("""
            SELECT checksum
            FROM rag_sources
            WHERE source_type=:st AND source_id=:sid
        """),
        {"st": source_type, "sid": source_id}
    ).fetchone()
    return bool(row and row[0] == checksum)


def mark_indexed(conn, source_type: str, source_id: str, checksum: str, meta: dict):
    conn.execute(
        text("""
            INSERT INTO rag_sources(source_type, source_id, checksum, indexed_at, meta)
            VALUES (:st, :sid, :chk, now(), CAST(:meta AS jsonb))
            ON CONFLICT (source_type, source_id)
            DO UPDATE SET checksum=EXCLUDED.checksum, indexed_at=now(), meta=EXCLUDED.meta
        """),
        {
            "st": source_type,
            "sid": source_id,
            "chk": checksum,
            "meta": json.dumps(meta, ensure_ascii=False)
        }
    )

# -----------------------------
# nettoyage et filtrage
# -----------------------------
def normalize_text(t: str) -> str:
    t = (t or "").replace("\x0c", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\n{3,}", "\n\n", t)

    lines = []
    for line in t.split("\n"):
        s = line.strip()
        if not s:
            lines.append("")
            continue
        if len(s) <= 2:
            continue
        if re.fullmatch(r"\d{1,4}", s):
            continue
        lines.append(s)

    t = "\n".join(lines)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t


def alpha_ratio(t: str) -> float:
    if not t:
        return 0.0
    letters = sum(ch.isalpha() for ch in t)
    total = sum(not ch.isspace() for ch in t)
    return (letters / total) if total else 0.0


def is_good_chunk(t: str) -> bool:
    t = (t or "").strip()
    if len(t) < MIN_CHUNK_CHARS:
        return False
    if alpha_ratio(t) < MIN_ALPHA_RATIO:
        return False
    return True


# -----------------------------
# Chunking 
# -----------------------------
def smart_chunks(text_in: str, max_chars: int = MAX_CHARS, overlap: int = OVERLAP) -> List[str]:
    text_in = (text_in or "").strip()
    if not text_in:
        return []

    text_in = re.sub(r"\n{3,}", "\n\n", text_in)
    paragraphs = [p.strip() for p in text_in.split("\n\n") if p.strip()]

    chunks: List[str] = []
    cur = ""

    for p in paragraphs:
        while len(p) > max_chars:
            part = p[:max_chars].strip()
            if part:
                chunks.append(part)
            p = p[max_chars - overlap:].strip()

        if not cur:
            cur = p
            continue

        if len(cur) + 2 + len(p) <= max_chars:
            cur = f"{cur}\n\n{p}"
        else:
            chunks.append(cur)
            tail = cur[-overlap:].strip() if overlap > 0 else ""
            cur = f"{tail}\n\n{p}".strip() if tail else p

    if cur:
        chunks.append(cur)

    return [c for c in chunks if is_good_chunk(c)]


# -----------------------------
# OCR page par page + prétraitement
# -----------------------------
def preprocess_for_ocr(img):
    img = ImageOps.grayscale(img)
    img = img.point(lambda x: 0 if x < 160 else 255, "1")
    return img


def ocr_pdf_pages(pdf_path: str) -> List[Tuple[int, str]]:
    images = convert_from_path(pdf_path, dpi=PDF_DPI)
    out: List[Tuple[int, str]] = []

    for i, img in enumerate(images):
        img = preprocess_for_ocr(img)
        t = pytesseract.image_to_string(img, lang=OCR_LANG, config=TESS_CONFIG)
        t = normalize_text(t)
        if t:
            out.append((i + 1, t))

    return out


def main():
    init_db()
    model = SentenceTransformer(MODEL_NAME)

    pdfs = glob.glob("../data/*.pdf")
    if not pdfs:
        print("⚠️ Aucun PDF trouvé dans ../data")
        return

    with engine.begin() as conn:
        ensure_rag_sources(conn)

        # pgvector adapter
        
        dbapi_conn = conn.connection.driver_connection if hasattr(conn.connection, "driver_connection") else conn.connection.connection
        register_vector(dbapi_conn)

        for pdf in pdfs:
            base = os.path.basename(pdf)
            chk = file_sha256(pdf)

            
            if already_indexed_same_checksum(conn, "pdf_ocr", base, chk):
                print(f"⏭️ Skip (inchangé) : {base}")
                continue

            print(f"📄 OCR + Index : {base}")

            pages = ocr_pdf_pages(pdf)
            if not pages:
                print(f" Aucun texte OCR détecté : {base}")
                continue

            
            conn.execute(
                text("DELETE FROM rag_chunks WHERE source_type='pdf_ocr' AND source_id=:sid"),
                {"sid": base}
            )

            total_chunks = 0

            for page_num, page_text in pages:
                chunks = smart_chunks(page_text, max_chars=MAX_CHARS, overlap=OVERLAP)
                if not chunks:
                    continue

                vectors = model.encode(chunks, normalize_embeddings=True)

                for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
                    conn.execute(
                        text("""
                            INSERT INTO rag_chunks(source_type, source_id, content, metadata, embedding)
                            VALUES (:st, :sid, :content, CAST(:meta AS jsonb), :emb)
                        """),
                        {
                            "st": "pdf_ocr",
                            "sid": base,
                            "content": chunk,
                            "meta": json.dumps(
                                {
                                    "file": base,
                                    "page": page_num,
                                    "chunk_in_page": idx,
                                    "ocr": True,
                                    "lang": OCR_LANG,
                                    "dpi": PDF_DPI,
                                    "psm": 6
                                },
                                ensure_ascii=False
                            ),
                            "emb": Vector(vec.tolist())
                        }
                    )
                total_chunks += len(chunks)

            
            mark_indexed(
                conn,
                "pdf_ocr",
                base,
                chk,
                {
                    "file": base,
                    "lang": OCR_LANG,
                    "dpi": PDF_DPI,
                    "psm": 6,
                    "pages": len(pages),
                    "chunks": total_chunks
                }
            )

            print(f"Indexé : {base} ({len(pages)} pages OCR, {total_chunks} chunks)")

    print("Ingestion OCR incrémentale terminée.")


if __name__ == "__main__":
    main()
