import sys, os, glob, json, re, hashlib
from typing import List, Tuple, Optional, Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pdf2image import convert_from_path
from PIL import ImageOps
import pytesseract
from sentence_transformers import SentenceTransformer
from sqlalchemy import text
from app.db import engine, init_db
from pgvector.psycopg2 import register_vector

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\rthabet\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
os.environ["TESSDATA_PREFIX"] = r"C:\Users\rthabet\AppData\Local\Programs\Tesseract-OCR\tessdata"

MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")

OCR_LANG = "fra+eng"
PDF_DPI = 400
TESS_CONFIG = "--oem 1 --psm 6"

MAX_CHARS = 1600
OVERLAP = 150

MIN_CHUNK_CHARS = 280
MIN_ALPHA_RATIO = 0.60
MAX_SPECIAL_RATIO = 0.20
MIN_SENTENCES = 2
MAX_SHORTLINE_RATIO = 0.55

RX_SPACES = re.compile(r"[ \t]+")
RX_MANY_NL = re.compile(r"\n{3,}")
RX_PAGE_NOISE = re.compile(r"^\s*\d{1,4}\s*$")
RX_SENT_SPLIT = re.compile(r"[.!?…]+")
RX_DATE = re.compile(r"\b(\d{2}\s*/\s*\d{2}\s*/\s*\d{4}|31\s*/\s*12\s*/\s*(19\d{2}|20\d{2}))\b", re.I)

RX_HEADING = re.compile(
    r"^\s*(?:"
    r"OPINION|FONDEMENT\s+DE\s+L['’]OPINION|RAPPORT\s+SUR\s+L['’]AUDIT|"
    r"RESPONSABILIT[ÉE]\s+DU\s+GESTIONNAIRE|RESPONSABILIT[ÉE]\s+DU\s+COMMISSAIRE|"
    r"OBJET\s+DU\s+RAPPORT|ETATS?\s+FINANCIERS?|ÉTATS?\s+FINANCIERS?|NOTE\s+\d+|"
    r"ANNEXE|BILAN|ETAT\s+DE\s+RESULTAT|ÉTAT\s+DE\s+R[ÉE]SULTAT|"
    r"ETAT\s+DE\s+VARIATION|ÉTAT\s+DE\s+VARIATION"
    r")\s*[:\-]?\s*$",
    re.I,
)

RX_ALLCAPS = re.compile(r"^[^a-z]{6,}$")

DOC_TYPE_RULES = [
    ("rapport_commissaire_comptes", re.compile(r"\b(rapport\s+du\s+commissaire\s+aux\s+comptes|commissaire\s+aux\s+comptes|opinion|certifions|audit\s+des\s+[ée]tats\s+financiers)\b", re.I)),
    ("reglement_interieur", re.compile(r"\b(r[ée]glement\s+int[ée]rieur|r[ée]glement\s+du\s+fonds|dispositions\s+g[ée]n[ée]rales)\b", re.I)),
    ("carte_fiscale", re.compile(r"\b(carte\s+fiscale|identifiant\s+fiscal|matricule\s+fiscal|code\s+tva)\b", re.I)),
    ("statuts", re.compile(r"\b(statuts|assembl[ée]e\s+g[ée]n[ée]rale|capital\s+social|objet\s+social)\b", re.I)),
    ("note_juridique", re.compile(r"\b(note\s+juridique|avis\s+juridique|conform[ée]ment\s+[àa]\s+la\s+loi|r[ée]f[ée]rence\s+l[ée]gale)\b", re.I)),
]


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
        text("SELECT checksum FROM rag_sources WHERE source_type=:st AND source_id=:sid"),
        {"st": source_type, "sid": source_id},
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
        {"st": source_type, "sid": source_id, "chk": checksum, "meta": json.dumps(meta, ensure_ascii=False)},
    )


def normalize_text(t: str) -> str:
    t = (t or "").replace("\x0c", " ")
    t = RX_SPACES.sub(" ", t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = RX_MANY_NL.sub("\n\n", t).strip()

    lines: List[str] = []
    for line in t.split("\n"):
        s = line.strip()
        if not s:
            lines.append("")
            continue
        if len(s) <= 2:
            continue
        if RX_PAGE_NOISE.fullmatch(s):
            continue
        lines.append(s)

    t = "\n".join(lines)
    t = RX_MANY_NL.sub("\n\n", t).strip()
    return t


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


def alpha_ratio(t: str) -> float:
    if not t:
        return 0.0
    letters = sum(ch.isalpha() for ch in t)
    total = sum(not ch.isspace() for ch in t)
    return (letters / total) if total else 0.0


def special_ratio(t: str) -> float:
    if not t:
        return 1.0
    specials = 0
    total = 0
    for ch in t:
        if ch.isspace():
            continue
        total += 1
        if not (ch.isalpha() or ch.isdigit()):
            specials += 1
    return (specials / total) if total else 1.0


def sentence_count(t: str) -> int:
    parts = [p.strip() for p in RX_SENT_SPLIT.split(t or "") if p.strip()]
    good = 0
    for p in parts:
        w = len(p.split())
        if w >= 4:
            good += 1
    return good


def short_line_ratio(t: str) -> float:
    lines = [ln.strip() for ln in (t or "").split("\n") if ln.strip()]
    if not lines:
        return 1.0
    short = 0
    for ln in lines:
        if len(ln.split()) < 5:
            short += 1
    return short / max(1, len(lines))


def is_good_chunk(t: str) -> bool:
    t = (t or "").strip()
    if len(t) < MIN_CHUNK_CHARS:
        return False
    if alpha_ratio(t) < MIN_ALPHA_RATIO:
        return False
    if special_ratio(t) > MAX_SPECIAL_RATIO:
        return False
    if sentence_count(t) < MIN_SENTENCES:
        return False
    if short_line_ratio(t) > MAX_SHORTLINE_RATIO:
        return False
    return True


def looks_like_heading(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False
    if RX_HEADING.match(s):
        return True
    if len(s) <= 80 and RX_ALLCAPS.match(s) and len(s.split()) <= 10:
        return True
    return False


def split_into_sections(page_text: str) -> List[str]:
    lines = [ln.rstrip() for ln in (page_text or "").split("\n")]
    sections: List[List[str]] = []
    cur: List[str] = []

    for ln in lines:
        s = ln.strip()
        if looks_like_heading(s) and cur:
            sections.append(cur)
            cur = [s]
            continue
        cur.append(ln)

    if cur:
        sections.append(cur)

    out: List[str] = []
    for sec in sections:
        txt = normalize_text("\n".join(sec))
        if txt:
            out.append(txt)
    return out


def chunk_section(section_text: str, max_chars: int = MAX_CHARS, overlap: int = OVERLAP) -> List[str]:
    s = (section_text or "").strip()
    if not s:
        return []
    if len(s) <= max_chars:
        return [s]

    chunks: List[str] = []
    i = 0
    while i < len(s):
        end = min(len(s), i + max_chars)
        cut = s.rfind("\n\n", i, end)
        if cut == -1 or cut <= i + int(max_chars * 0.5):
            cut = s.rfind(". ", i, end)
        if cut == -1 or cut <= i + int(max_chars * 0.5):
            cut = end

        part = s[i:cut].strip()
        if part:
            chunks.append(part)

        if cut >= len(s):
            break

        i = max(0, cut - overlap)

    return chunks


def structured_chunks(page_text: str) -> List[str]:
    sections = split_into_sections(page_text)
    out: List[str] = []
    for sec in sections:
        out.extend(chunk_section(sec, MAX_CHARS, OVERLAP))
    return [c for c in out if is_good_chunk(c)]


def detect_document_type(chunk_text: str, page_num: int) -> str:
    t = (chunk_text or "")
    for doc_type, rx in DOC_TYPE_RULES:
        if rx.search(t):
            if doc_type == "rapport_commissaire_comptes":
                if page_num <= 3 or RX_DATE.search(t) or re.search(r"\b(etats?\s+financiers?|bilan|exercice\s+clos)\b", t, re.I):
                    return doc_type
            else:
                return doc_type
    if re.search(r"\b(commissaire\s+aux\s+comptes|opinion|audit)\b", t, re.I):
        return "rapport_commissaire_comptes"
    if re.search(r"\b(r[ée]glement\s+int[ée]rieur)\b", t, re.I):
        return "reglement_interieur"
    if re.search(r"\b(statuts)\b", t, re.I):
        return "statuts"
    if re.search(r"\b(carte\s+fiscale|matricule\s+fiscal)\b", t, re.I):
        return "carte_fiscale"
    if re.search(r"\b(avis\s+juridique|note\s+juridique)\b", t, re.I):
        return "note_juridique"
    return "note_juridique"


def main():
    init_db()
    model = SentenceTransformer(MODEL_NAME)

    pdfs = glob.glob("../data/*.pdf")
    if not pdfs:
        print(" Aucun PDF trouvé dans ../data")
        return

    with engine.begin() as conn:
        ensure_rag_sources(conn)

        dbapi_conn = conn.connection.driver_connection if hasattr(conn.connection, "driver_connection") else conn.connection.connection
        register_vector(dbapi_conn)

        for pdf in pdfs:
            base = os.path.basename(pdf)
            chk = file_sha256(pdf)

            if already_indexed_same_checksum(conn, "pdf_ocr", base, chk):
                print(f" Skip (inchangé) : {base}")
                continue

            print(f"OCR + Index : {base}")

            pages = ocr_pdf_pages(pdf)
            if not pages:
                print(f" Aucun texte OCR détecté : {base}")
                continue

            conn.execute(
                text("DELETE FROM rag_chunks WHERE source_type='pdf_ocr' AND source_id=:sid"),
                {"sid": base},
            )

            total_chunks = 0
            kept_chunks = 0

            for page_num, page_text in pages:
                chunks = structured_chunks(page_text)
                if not chunks:
                    continue

                metas: List[Dict[str, Any]] = []
                final_chunks: List[str] = []
                for idx, chunk in enumerate(chunks):
                    if not is_good_chunk(chunk):
                        continue
                    dt = detect_document_type(chunk, page_num)
                    meta = {
                        "file": base,
                        "page": page_num,
                        "chunk_in_page": idx,
                        "ocr": True,
                        "lang": OCR_LANG,
                        "dpi": PDF_DPI,
                        "psm": 6,
                        "document_type": dt,
                    }
                    final_chunks.append(chunk)
                    metas.append(meta)

                total_chunks += len(chunks)
                kept_chunks += len(final_chunks)

                if not final_chunks:
                    continue

                vectors = model.encode(final_chunks, normalize_embeddings=True)

                for chunk, meta, vec in zip(final_chunks, metas, vectors):
                    conn.execute(
                        text("""
                            INSERT INTO rag_chunks(source_type, source_id, content, metadata, embedding)
                            VALUES (:st, :sid, :content, CAST(:meta AS jsonb), CAST(:emb AS vector))
                        """),
                        {
                            "st": "pdf_ocr",
                            "sid": base,
                            "content": chunk,
                            "meta": json.dumps(meta, ensure_ascii=False),
                            "emb": vec.tolist(),
                        },
                    )

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
                    "chunks_total": total_chunks,
                    "chunks_kept": kept_chunks,
                },
            )

            print(f"Indexé : {base} ({len(pages)} pages, chunks: {kept_chunks}/{total_chunks})")

    print("Ingestion OCR incrémentale terminée.")


if __name__ == "__main__":
    main()
