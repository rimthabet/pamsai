# rag/retrieve_core.py
from __future__ import annotations

import os, re, json
from typing import Any, Dict, List, Optional, Tuple

from sentence_transformers import SentenceTransformer
from sqlalchemy import text

from app.db import engine
from pgvector.psycopg2 import register_vector

MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")

# Cache global du modèle embedding (perf)
_EMBED_MODEL: Optional[SentenceTransformer] = None


def _get_embed_model() -> SentenceTransformer:
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        _EMBED_MODEL = SentenceTransformer(MODEL_NAME)
    return _EMBED_MODEL


def _get_dbapi_conn(conn):
    try:
        return conn.connection.driver_connection
    except Exception:
        return conn.connection.connection


# -----------------------------
# Domain detection 
# -----------------------------
INTENT_RULES: List[Tuple[str, int, re.Pattern]] = [
    ("document", 100, re.compile(
        r"\b(commission|commissions|prospectus|note|r[eè]glement|rapport|annexe|risque|risques|pv|proc[eè]s[-\s]?verbal|document)\b",
        re.I
    )),
    ("fonds", 60, re.compile(
        r"\b(fonds|fcpr|fcp|montant du fonds|taille du fonds|duree du fonds|durée du fonds|"
        r"frais de gestion|frais gestion|frais dépositaire|frais depositaire|ratio|visa|cmf|agrement|agrément)\b",
        re.I
    )),
    ("projet", 50, re.compile(
        r"\b(projet|pipeline|prospection|préselection|présélection|etude|étude|business\s*plan|bp|activité|activite|capital)\b",
        re.I
    )),
    ("souscription", 40, re.compile(
        r"\b(souscription|souscriptions|souscrire|souscripteur|souscripteurs|investisseur|investisseurs|engagement)\b",
        re.I
    )),
    ("liberation", 40, re.compile(
        r"\b(libération|liberation|libérations|liberations|appel\s+de\s+fonds|tirage|versement)\b",
        re.I
    )),
]

DOMAIN_TO_SOURCE_TYPES: Dict[str, List[str]] = {
    "fonds": ["maxula:fonds"],
    "projet": ["maxula:projet"],
    "souscription": ["maxula:souscription"],
    "liberation": ["maxula:liberation"],
    "document": ["pdf_ocr", "maxula:document"],
}


def detect_domain(query: str) -> str:
    q = query or ""
    hits: List[Tuple[int, str]] = []
    for domain, prio, rx in INTENT_RULES:
        if rx.search(q):
            hits.append((prio, domain))
    if not hits:
        return ""
    hits.sort(reverse=True)
    return hits[0][1]


def scope_for_domain(domain: str) -> List[str]:
    return DOMAIN_TO_SOURCE_TYPES.get(domain, [])


def auto_source_types(query: str) -> List[str]:
    """
    Déduit un scope (liste de source_types) depuis la requête.
    Peut retourner multi-scope (ex: document+fonds).
    """
    q = query or ""
    hits: List[Tuple[int, str]] = []
    for domain, prio, rx in INTENT_RULES:
        if rx.search(q):
            hits.append((prio, domain))

    if not hits:
        return []

    hits.sort(reverse=True)
    top_prio = hits[0][0]
    top_domains = [d for p, d in hits if p == top_prio]

    if re.search(r"\bfrais\b", q, re.I) and re.search(r"\bcommission", q, re.I):
        top_domains = ["document", "fonds"]

    sts: List[str] = []
    for d in top_domains:
        sts.extend(DOMAIN_TO_SOURCE_TYPES.get(d, []))

    out: List[str] = []
    for s in sts:
        if s and s not in out:
            out.append(s)
    return out


# -----------------------------
# Entity hint extraction 
# -----------------------------
def extract_entity_hint(query: str) -> str:
    q = (query or "").strip()
    m = re.search(r'"([^"]{3,})"', q)
    if m:
        return m.group(1).strip()
    m = re.search(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", q, re.I)
    if m:
        return m.group(2).strip().strip('"').strip("'")
    return ""


# -----------------------------
# Keyword retrieval
# -----------------------------
def keyword_lookup(entity_hint: str, source_types: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    hint = (entity_hint or "").strip()
    if not hint or len(hint) < 3:
        return []

    where = ["content ILIKE :pat"]
    params: Dict[str, Any] = {"pat": f"%{hint}%", "k": limit}

    if source_types:
        where.append("source_type = ANY(:sts)")
        params["sts"] = source_types

    sql = text(f"""
        SELECT id, source_type, source_id, metadata, content, 1.0 AS score
        FROM rag_chunks
        WHERE {" AND ".join(where)}
        LIMIT :k
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, params).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        meta = r["metadata"]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        out.append({
            "id": r["id"],
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "metadata": meta or {},
            "content": r["content"],
            "score": float(r["score"]),
        })
    return out


# ----------------------------
# Semantic retrieval (pgvector)
# -----------------------------
def semantic_retrieve(query: str, top_k: int = 8, source_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    model = _get_embed_model()
    qvec = model.encode([query], normalize_embeddings=True)[0].tolist()

    where = []
    params: Dict[str, Any] = {"qvec": qvec, "k": top_k}

    if source_types:
        where.append("source_type = ANY(:sts)")
        params["sts"] = source_types

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = text(f"""
        SELECT
            id, source_type, source_id, metadata, content,
            (1 - (embedding <=> (:qvec)::vector)) AS score
        FROM rag_chunks
        {where_sql}
        ORDER BY embedding <=> (:qvec)::vector
        LIMIT :k
    """)

    with engine.begin() as conn:
        register_vector(_get_dbapi_conn(conn))
        rows = conn.execute(sql, params).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        meta = r["metadata"]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        out.append({
            "id": r["id"],
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "metadata": meta or {},
            "content": r["content"],
            "score": float(r["score"]),
        })
    return out


# -----------------------------
# Hybrid retrieve 
# -----------------------------
def hybrid_retrieve(
    query: str,
    top_k: int = 8,
    source_types: Optional[List[str]] = None,
    entity_hint: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], List[str], str]:
    """
    Hybrid:
    - scope auto si source_types non fourni
    - keyword boost si entity_hint trouvé
    - semantic retrieve
    - merge + dedupe + tri score desc
    """
    domain = detect_domain(query)
    auto_scope = auto_source_types(query)
    scope = source_types if source_types is not None else auto_scope

    if scope is None:
        scope = []

    hint = (entity_hint or "").strip() or extract_entity_hint(query)

    kw = keyword_lookup(hint, scope, limit=min(5, top_k)) if hint else []
    sem = semantic_retrieve(query, top_k=top_k, source_types=scope if scope else None)

    best: Dict[int, Dict[str, Any]] = {}
    for ch in kw + sem:
        cid = int(ch["id"])
        if cid not in best or float(ch["score"]) > float(best[cid]["score"]):
            best[cid] = ch

    merged = sorted(best.values(), key=lambda x: float(x.get("score", 0.0)), reverse=True)[:top_k]
    return merged, scope, domain
