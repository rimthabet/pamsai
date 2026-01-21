import sys, os, json, argparse, re, time, unicodedata
from typing import Any, Dict, List, Tuple, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests
from sentence_transformers import SentenceTransformer
from sqlalchemy import text

from app.db import engine
from pgvector.psycopg2 import register_vector


# =========================
# CONFIG
# =========================
MODEL_NAME = os.getenv("EMBED_MODEL", "BAAI/bge-m3")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2")

DEFAULT_TOPK = int(os.getenv("RAG_TOPK", "10"))
KW_LIMIT = int(os.getenv("RAG_KEYWORD_LIMIT", "5"))
OLLAMA_RETRIES = int(os.getenv("OLLAMA_RETRIES", "2"))
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))


# =========================
# TEXT NORMALIZATION 
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


# =========================
# AUTO-SCOPE 
# =========================
INTENT_RULES = [
    ("document", 100, re.compile(r"\b(commission|commissions|prospectus|note|r[eè]glement|rapport|annexe|risque|risques|pv|proc[eè]s[-\s]?verbal|document)\b", re.I)),
    ("fonds", 60, re.compile(r"\b(fonds|fcpr|fcp|montant du fonds|taille du fonds|duree du fonds|durée du fonds|frais de gestion|frais gestion|frais dépositaire|frais depositaire|ratio|visa|cmf|agrement|agrément)\b", re.I)),
    ("projet", 50, re.compile(r"\b(projet|pipeline|prospection|préselection|présélection|etude|étude|business\s*plan|bp|activité|activite)\b", re.I)),
    ("souscription", 40, re.compile(r"\b(souscription|souscriptions|souscrire|souscripteur|souscripteurs|investisseur|investisseurs|engagement)\b", re.I)),
    ("liberation", 40, re.compile(r"\b(libération|liberation|libérations|liberations|appel\s+de\s+fonds|tirage|versement)\b", re.I)),
]

DOMAIN_TO_SOURCE_TYPES: Dict[str, List[str]] = {
    "fonds": ["maxula:fonds"],
    "projet": ["maxula:projet"],
    "souscription": ["maxula:souscription"],
    "liberation": ["maxula:liberation"],
    "document": ["pdf_ocr", "maxula:document"],
}

def detect_domain(question: str) -> str:
    q = question or ""
    hits = []
    for domain, prio, rx in INTENT_RULES:
        if rx.search(q):
            hits.append((prio, domain))
    if not hits:
        return ""
    hits.sort(reverse=True)
    return hits[0][1]

def scope_for_domain(domain: str) -> List[str]:
    return DOMAIN_TO_SOURCE_TYPES.get(domain, [])


# =========================
# DB/PGVECTOR helpers
# =========================
def _get_dbapi_conn(conn):
    try:
        return conn.connection.driver_connection
    except Exception:
        return conn.connection.connection


# =========================
# HYBRID RETRIEVAL
# =========================
def extract_entity_hint(question: str) -> str:
    """
    Extrait un nom probable d'entité:
    - "..." entre guillemets
    - nommé/appelé X
    - fallback: suite de mots avec Majuscules
    """
    q = (question or "").strip()

    m = re.search(r'"([^"]{3,})"', q)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", q, re.I)
    if m:
        return m.group(2).strip().strip('"').strip("'")

    m = re.search(r"\b([A-Z][A-Za-z0-9&'’\- ]{3,})\b", q)
    if m:
        return m.group(1).strip()

    return ""

def keyword_lookup(question: str, source_types: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    """
    Keyword retrieval (ILIKE) dans rag_chunks.content.
    """
    hint = extract_entity_hint(question)
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

    out = []
    for r in rows:
        meta = r["metadata"]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                pass
        out.append({
            "id": r["id"],
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "metadata": meta,
            "content": r["content"],
            "score": float(r["score"]),
        })
    return out

def semantic_retrieve(question: str, top_k: int, source_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    model = SentenceTransformer(MODEL_NAME)
    qvec = Vector(model.encode([question], normalize_embeddings=True)[0].tolist())

    where = []
    params: Dict[str, Any] = {"qvec": qvec, "k": top_k}
    if source_types:
        where.append("source_type = ANY(:sts)")
        params["sts"] = source_types

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = text(f"""
        SELECT id, source_type, source_id, metadata, content,
               (1 - (embedding <=> (:qvec)::vector)) AS score
        FROM rag_chunks
        {where_sql}
        ORDER BY embedding <=> (:qvec)::vector
        LIMIT :k
    """)

    with engine.begin() as conn:
        register_vector(_get_dbapi_conn(conn))
        rows = conn.execute(sql, params).mappings().all()

    out = []
    for r in rows:
        meta = r["metadata"]
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                pass
        out.append({
            "id": r["id"],
            "source_type": r["source_type"],
            "source_id": r["source_id"],
            "metadata": meta,
            "content": r["content"],
            "score": float(r["score"]),
        })
    return out

def retrieve(question: str, top_k: int) -> Tuple[List[Dict[str, Any]], List[str], str]:
    domain = detect_domain(question)
    scope = scope_for_domain(domain)

    if domain == "document":
        scope = ["pdf_ocr", "maxula:document"]

    kw = keyword_lookup(question, scope, limit=min(KW_LIMIT, top_k))
    sem = semantic_retrieve(question, top_k=top_k, source_types=scope if scope else None)

    seen = set()
    merged: List[Dict[str, Any]] = []
    for ch in kw + sem:
        if ch["id"] in seen:
            continue
        seen.add(ch["id"])
        merged.append(ch)
        if len(merged) >= top_k:
            break

    return merged, scope, domain


# =========================
# CONTEXT PACKING
# =========================
def extract_kv_from_db_text(content: str) -> Dict[str, str]:
    """
    Parse: "TABLE=... | PK=... | col=val | col=val"
    """
    kv: Dict[str, str] = {}
    parts = [p.strip() for p in (content or "").split("|")]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
    return kv

def compact_source(i: int, ch: Dict[str, Any]) -> str:
    st = ch["source_type"]
    meta = ch.get("metadata") or {}
    score = ch["score"]

    if st.startswith("maxula:"):
        kv = extract_kv_from_db_text(ch.get("content", ""))
        table = meta.get("table") if isinstance(meta, dict) else None
        pk = meta.get("pk") if isinstance(meta, dict) else None

        keys_priority = [
            "nom", "denomination", "raison_sociale", "alias",
            "activite", "activité", "secteur", "domaine",
            "montant", "duree", "capital_social", "capital",
            "frais_gestion", "frais_depositaire", "num_visa_cmf",
            "date_lancement", "date_visa_cmf", "statut",
        ]
        keep: Dict[str, str] = {}
        for k in keys_priority:
            if k in kv:
                keep[k] = kv[k]

        if keep:
            fields_str = "; ".join([f"{k}={v}" for k, v in keep.items()])
            return f"[S{i}] score={score:.3f} SOURCE={st}|{ch['source_id']} table={table} pk={pk}\nFIELDS: {fields_str}\n"

        snippet = (ch.get("content", "")[:450]).replace("\n", " ")
        return f"[S{i}] score={score:.3f} SOURCE={st}|{ch['source_id']} table={table} pk={pk}\nSNIPPET: {snippet}\n"

    file = meta.get("file") if isinstance(meta, dict) else None
    page = meta.get("page") if isinstance(meta, dict) else None
    txt = (ch.get("content", "")[:1500]).strip()
    return f"[S{i}] score={score:.3f} SOURCE={st}|{ch['source_id']} file={file} page={page}\nCONTENT:\n{txt}\n"

def build_prompt(question: str, chunks: List[Dict[str, Any]]) -> str:
    context_block = "\n".join([compact_source(i, ch) for i, ch in enumerate(chunks, start=1)])
    return f"""
Tu es un assistant métier pour une plateforme de gestion de fonds (PAMS).
Tu dois répondre UNIQUEMENT à partir des SOURCES fournies.
Si l’information n’existe pas dans les sources, réponds exactement :
"Je ne sais pas d’après les documents disponibles."

Règles:
- Réponds en français.
- Réponse courte et précise.
- Cite toujours [Sx] pour chaque information factuelle.
- Ne jamais inventer de chiffres, dates, noms ou champs.
- Si une source contient explicitement FIELDS: champ=valeur, utilise-la.
- Si la question est ambiguë, pose UNE question de clarification.

QUESTION:
{question}

SOURCES:
{context_block}

RÉPONSE:
""".strip()


# =========================
# STRUCTURED ANSWERING 
# =========================
FIELD_SYNONYMS: Dict[str, List[str]] = {
    # identité
    "nom": ["nom", "denomination", "dénomination", "raison sociale", "raison_sociale"],
    "alias": ["alias", "code", "sigle"],
    "statut": ["statut", "etat", "état"],

    # fonds
    "montant": ["montant", "taille", "encours"],
    "duree": ["duree", "durée"],
    "frais_gestion": ["frais de gestion", "frais gestion"],
    "frais_depositaire": ["frais depositaire", "frais dépositaire"],
    "num_visa_cmf": ["visa", "cmf", "num visa", "num_visa_cmf"],
    "date_lancement": ["date lancement", "date de lancement", "lancement"],

    # projets
    "activite": ["activite", "activité", "secteur", "domaine"],
    "capital_social": ["capital social", "capital_social", "capital", "capitalisation"],
}

def guess_field_from_question(question: str, available_fields: List[str]) -> Optional[str]:
    """
    Choix du champ par SCORING (robuste):
    - match champ direct dans question
    - match synonyms (accents off)
    - boosts par mots-clés importants (activite, montant, frais, etc.)
    """
    q = _norm(question)
    avail_map = {_norm(f): f for f in available_fields}
    scores: Dict[str, int] = {f: 0 for f in available_fields}

    #si le champ est explicitement cité
    for f in available_fields:
        fn = _norm(f).replace("_", " ")
        if fn and fn in q:
            scores[f] += 6

    # via synonyms
    for canonical, syns in FIELD_SYNONYMS.items():
        for s in syns:
            sn = _norm(s)
            if sn and sn in q:
                
                if canonical in available_fields:
                    scores[canonical] += 12
                
                if _norm(canonical) in avail_map:
                    scores[avail_map[_norm(canonical)]] += 12

                s_key = sn.replace(" ", "_")
                if s_key in avail_map:
                    scores[avail_map[s_key]] += 10
                if sn in avail_map:
                    scores[avail_map[sn]] += 10

    #mots-clés 
    BOOSTS = [
        ("activite", ["activite", "activite du projet", "secteur", "domaine"], 25),
        ("montant", ["montant", "taille", "encours"], 18),
        ("frais_gestion", ["frais de gestion", "frais gestion"], 18),
        ("frais_depositaire", ["frais depositaire", "frais depositaire"], 18),
        ("capital_social", ["capital", "capital social"], 15),
        ("duree", ["duree", "durée"], 12),
    ]
    for field, words, bonus in BOOSTS:
        if any(_norm(w) in q for w in words):
            # boost direct si field existe
            if field in available_fields:
                scores[field] += bonus
            # sinon boost sur key normalisée
            if _norm(field) in avail_map:
                scores[avail_map[_norm(field)]] += bonus

    best_field = max(scores, key=lambda k: scores[k])
    if scores[best_field] <= 0:
        return None
    return best_field

def structured_answer_from_chunks(question: str, chunks: List[Dict[str, Any]]) -> Optional[str]:
    """
    Réponse déterministe quand l'info existe dans les chunks DB.
    - choisit le chunk qui matche le mieux l'ENTITY_HINT si possible
    - choisit le champ via scoring
    """
    if not chunks:
        return None

    hint = _norm(extract_entity_hint(question))
    candidates = []

    for ch in chunks:
        if not ch["source_type"].startswith("maxula:"):
            continue
        kv = extract_kv_from_db_text(ch.get("content", ""))
        if not kv:
            continue
        name = _norm(kv.get("nom") or kv.get("denomination") or kv.get("raison_sociale") or kv.get("alias") or "")
        # score chunk
        cs = 0
        if hint and name and hint in name:
            cs += 50
        cs += int(ch.get("score", 0) * 10)  
        candidates.append((cs, ch, kv))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_chunk, kv = candidates[0]

    field = guess_field_from_question(question, list(kv.keys()))
    if not field:
        return None
    val = kv.get(field)
    if val is None or str(val).strip() == "":
        return None

    name = kv.get("nom") or kv.get("denomination") or kv.get("raison_sociale") or kv.get("alias")
    label = f" **{name}**" if name else ""
    field_label = field.replace("_", " ")

    # trouver index source Sx
    s_idx = 1
    for i, ch in enumerate(chunks, start=1):
        if ch["id"] == best_chunk["id"]:
            s_idx = i
            break

    return f"{field_label.capitalize()} de{label} : **{val}**. [S{s_idx}]"


# =========================
# Ollama (api/chat + retry)
# =========================
def ollama_chat(prompt: str, retries: int = 2) -> str:
    url = f"{OLLAMA_URL}/api/chat"
    payload = {
        "model": LLM_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": "Tu es un assistant RAG strict : pas d'hallucination, citations obligatoires."},
            {"role": "user", "content": prompt},
        ],
        "options": {"temperature": 0.1}
    }

    for _ in range(retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            content = ((data.get("message") or {}).get("content") or "").strip()
            if content:
                return content
            time.sleep(0.4)
        except Exception:
            time.sleep(0.4)
    return ""


# =========================
# CLI
# =========================
def main():
    parser = argparse.ArgumentParser(description="RAG Answering (Hybrid + Structured + Llama3.2 via Ollama)")
    parser.add_argument("question", type=str)
    parser.add_argument("-k", "--top_k", type=int, default=DEFAULT_TOPK)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    chunks, scope, domain = retrieve(args.question, top_k=args.top_k)

    print("\n====================================")
    print("QUESTION:", args.question)
    print("TOP_K:", args.top_k)
    print("DOMAIN:", domain if domain else "(unknown)")
    print("AUTO_SCOPE:", scope if scope else "(global)")
    print("ENTITY_HINT:", extract_entity_hint(args.question) or "(none)")
    print("====================================\n")

    if not chunks:
        print("Aucune source trouvée.")
        return

    # Structured Answering 
    sa = structured_answer_from_chunks(args.question, chunks)
    if sa:
        if args.debug:
            print("--- DEBUG structured path: used ---\n")
        print("RÉPONSE:\n")
        print(sa)
    else:
        # LLM Answering (RAG)
        prompt = build_prompt(args.question, chunks)
        if args.debug:
            print("\n--- DEBUG prompt length:", len(prompt), "---")
            print("\n--- DEBUG first 2 sources ---")
            for i, ch in enumerate(chunks[:2], start=1):
                print(compact_source(i, ch))

        answer = ollama_chat(prompt, retries=OLLAMA_RETRIES)
        print("RÉPONSE:\n")
        print(answer if answer else "(⚠️ Réponse vide du LLM — vérifier Ollama / modèle.)")

    print("\n--- SOURCES (résumé) ---")
    for i, ch in enumerate(chunks, start=1):
        meta = ch.get("metadata") or {}
        line = f"[S{i}] score={ch['score']:.3f} | {ch['source_type']} | {ch['source_id']}"
        if isinstance(meta, dict) and meta.get("file") and meta.get("page"):
            line += f" | {meta.get('file')} p.{meta.get('page')}"
        if isinstance(meta, dict) and meta.get("table") and meta.get("pk"):
            line += f" | table={meta.get('table')} pk={meta.get('pk')}"
        print(line)

if __name__ == "__main__":
    main()
