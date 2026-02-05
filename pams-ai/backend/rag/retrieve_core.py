from __future__ import annotations

import os, re, json
from typing import Any, Dict, List, Optional, Tuple
from functools import lru_cache

from sentence_transformers import SentenceTransformer
from sqlalchemy import text

from app.db import engine
from pgvector.psycopg2 import register_vector

MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")
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


RX_DOC_TITLE = re.compile(
    r"\b("
    r"situation\s+annuelle|"
    r"rapport\s+du\s+commissaire|"
    r"commissaire\s+aux\s+comptes|"
    r"etats?\s+financiers?|"
    r"états?\s+financiers?|"
    r"arretee?\s+au|arrêtée?\s+au|"
    r"exercice\s+clos|"
    r"bilan\s+au|"
    r"valeur\s+liquidative|"
    r"actif\s+net|"
    r"\b31/12/\d{4}\b|"
    r"\b\d{2}/\d{2}/\d{4}\b"
    r")\b",
    re.I,
)

RX_FONDS_WORDS = re.compile(r"\b(fonds|fcpr|fcp|sicav)\b", re.I)
RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

# extraire un nom de fonds 
RX_FONDS_NAME = re.compile(
    r"\b((?:FCPR|FCP|SICAV)\s+[A-Z0-9][A-Z0-9\s\-\._]{4,})\b",
    re.I
)


def _looks_like_document_title(q: str) -> bool:
    q = (q or "").strip()
    if not q or len(q) < 8:
        return False
    if RX_DOC_TITLE.search(q):
        return True
    letters = [c for c in q if c.isalpha()]
    if len(letters) >= 12:
        upper_ratio = sum(c.isupper() for c in letters) / max(1, len(letters))
        if upper_ratio >= 0.55 and RX_FONDS_WORDS.search(q):
            return True
    return False


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
    if _looks_like_document_title(q):
        return "document"
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
    q = query or ""
    looks_doc = _looks_like_document_title(q)

    hits: List[Tuple[int, str]] = []
    for domain, prio, rx in INTENT_RULES:
        if rx.search(q):
            hits.append((prio, domain))

    if not hits:
        if looks_doc:
            return ["pdf_ocr", "maxula:document"]
        return []

    hits.sort(reverse=True)
    top_prio = hits[0][0]
    top_domains = [d for p, d in hits if p == top_prio]

    if re.search(r"\bfrais\b", q, re.I) and re.search(r"\bcommission", q, re.I):
        top_domains = ["document", "fonds"]

    if looks_doc and "document" not in top_domains:
        top_domains = ["document"] + top_domains

    sts: List[str] = []
    for d in top_domains:
        sts.extend(DOMAIN_TO_SOURCE_TYPES.get(d, []))

    out: List[str] = []
    for s in sts:
        if s and s not in out:
            out.append(s)
    return out


def extract_entity_hint(query: str) -> str:
    q = (query or "").strip()
    m = re.search(r'"([^"]{3,})"', q)
    if m:
        return m.group(1).strip()
    m = re.search(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", q, re.I)
    if m:
        return m.group(2).strip().strip('"').strip("'")
    return ""


def _extract_year(query: str) -> Optional[int]:
    m = RX_YEAR.search(query or "")
    return int(m.group(1)) if m else None


def _extract_fonds_name(query: str) -> str:
    q = (query or "").strip()
    m = RX_FONDS_NAME.search(q)
    if m:
        # normaliser espaces
        return re.sub(r"\s{2,}", " ", m.group(1).strip())
    return ""


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


@lru_cache(maxsize=512)
def _cached_query_vec(query: str) -> Tuple[float, ...]:
    model = _get_embed_model()
    v = model.encode([query], normalize_embeddings=True)[0].tolist()
    return tuple(float(x) for x in v)


def semantic_retrieve(
    query: str,
    top_k: int = 10,
    source_types: Optional[List[str]] = None,
    extra_where: Optional[List[str]] = None,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    qvec = list(_cached_query_vec(query))

    where = []
    params: Dict[str, Any] = {"qvec": qvec, "k": top_k}

    if source_types:
        where.append("source_type = ANY(:sts)")
        params["sts"] = source_types

    if extra_where:
        where.extend(extra_where)
    if extra_params:
        params.update(extra_params)

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


def _merge_dedupe(rows: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    best: Dict[int, Dict[str, Any]] = {}
    for ch in rows:
        cid = int(ch["id"])
        if cid not in best or float(ch["score"]) > float(best[cid]["score"]):
            best[cid] = ch
    return sorted(best.values(), key=lambda x: float(x.get("score", 0.0)), reverse=True)[:top_k]


def _lexical_rerank(rows: List[Dict[str, Any]], query: str, domain: str) -> List[Dict[str, Any]]:
    """
    Rerank léger (sans nouveau modèle) : boost si le chunk contient l'année / le nom du fonds / mots-clés.
    Très efficace sur OCR.
    """
    q = (query or "")
    qlow = q.lower()
    year = _extract_year(q)
    fonds = _extract_fonds_name(q)
    fonds_low = fonds.lower() if fonds else ""

    key_terms = ["actif net", "valeur liquidative", "bilan", "31/12", "arrete", "arrêté", "etats financiers", "états financiers"]

    def bonus(ch: Dict[str, Any]) -> float:
        b = 0.0
        content = (ch.get("content") or "")
        source_id = (ch.get("source_id") or "")
        txt = (content + " " + source_id).lower()

        
        if year:
            if str(year) in txt:
                b += 0.10
            if f"31/12/{year}" in txt or f"31-12-{year}" in txt:
                b += 0.12

        if fonds_low and len(fonds_low) >= 6:
            if fonds_low in txt:
                b += 0.18
            else:
                
                parts = [p for p in re.split(r"\s+", fonds_low) if len(p) >= 4]
                hit = sum(1 for p in parts[:4] if p in txt)
                if hit >= 2:
                    b += 0.10

        
        hits = sum(1 for t in key_terms if t in txt)
        b += min(0.12, hits * 0.03)

        
        if domain == "document":
            if ch.get("source_type") == "pdf_ocr":
                b += 0.04

        return b

    rescored = []
    for ch in rows:
        s = float(ch.get("score", 0.0))
        s2 = s + bonus(ch)
        ch2 = dict(ch)
        ch2["score"] = float(s2)
        rescored.append(ch2)

    rescored.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return rescored


def hybrid_retrieve(
    query: str,
    top_k: int = 10,
    source_types: Optional[List[str]] = None,
    entity_hint: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], List[str], str]:

    domain = detect_domain(query)
    auto_scope = auto_source_types(query)
    scope = source_types if source_types is not None else auto_scope
    if scope is None:
        scope = []

    hint = (entity_hint or "").strip() or extract_entity_hint(query)

  
    kw = keyword_lookup(hint, scope, limit=min(5, top_k)) if hint else []

   
    pool_k = max(30, top_k * 6)

  
    extra_where: List[str] = []
    extra_params: Dict[str, Any] = {}

    if domain == "document":
        year = _extract_year(query)
        fonds = _extract_fonds_name(query)

       
        if year:
            extra_where.append("(source_id ILIKE :y1 OR source_id ILIKE :y2 OR content ILIKE :y3)")
            extra_params["y1"] = f"%{year}%"
            extra_params["y2"] = f"%31/12/{year}%"
            extra_params["y3"] = f"%{year}%"

        
        if fonds and len(fonds) >= 6:
            extra_where.append("content ILIKE :fonds")
            extra_params["fonds"] = f"%{fonds}%"

    sem = semantic_retrieve(
        query,
        top_k=pool_k,
        source_types=scope if scope else None,
        extra_where=extra_where if extra_where else None,
        extra_params=extra_params if extra_params else None,
    )

    merged_pool = _merge_dedupe(kw + sem, top_k=pool_k)

    
    merged_pool = _lexical_rerank(merged_pool, query=query, domain=domain)

    merged = merged_pool[:top_k]

    best_score = float(merged[0]["score"]) if merged else 0.0
    looks_doc = _looks_like_document_title(query)
    has_pdf = any(r.get("source_type") == "pdf_ocr" for r in merged)

    
    if looks_doc and (not has_pdf):
        pdf_scope = ["pdf_ocr"]
        sem2 = semantic_retrieve(query, top_k=pool_k, source_types=pdf_scope)
        merged2 = _merge_dedupe(sem2 + merged_pool, top_k=pool_k)
        merged2 = _lexical_rerank(merged2, query=query, domain="document")
        merged = merged2[:top_k]
        for s in pdf_scope:
            if s not in scope:
                scope.append(s)


    if looks_doc and best_score < 0.50:
        pdf_scope = ["pdf_ocr"]
        sem2 = semantic_retrieve(query, top_k=pool_k, source_types=pdf_scope)
        merged2 = _merge_dedupe(sem2 + merged_pool, top_k=pool_k)
        merged2 = _lexical_rerank(merged2, query=query, domain="document")
        merged = merged2[:top_k]
        for s in pdf_scope:
            if s not in scope:
                scope.append(s)

    return merged, scope, domain
