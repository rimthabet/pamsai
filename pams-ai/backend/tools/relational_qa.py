# tools/relational_qa.py
from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text

from app.db import engine
from tools.schema_cache import get_schema_graph
from tools.join_resolver import shortest_path_fk


NAME_COL_CANDIDATES = ["nom", "denomination", "alias", "libelle", "raison_sociale", "name", "title"]


LABEL_COL_CANDIDATES = ["nom", "libelle", "denomination", "raison_sociale", "prenom", "name", "title"]

RX_ENTITY = re.compile(
    r'\b(nomm[eé]|appel[eé]|alias)\s+(?:"([^"]+)"|\'([^\']+)\'|(.+?))(\?|$)',
    re.I
)

def extract_entity_name(q: str) -> str:
    q = (q or "").strip()
    m = re.search(r'"([^"]{2,})"', q)
    if m:
        return m.group(1).strip()
    m = RX_ENTITY.search(q)
    if m:
        return (m.group(2) or m.group(3) or m.group(4) or "").strip().strip('"').strip("'")
    
    m = re.search(r"\b(du|de la|de l'|des)\s+(fonds|fond|projet)\s+(.+?)(\?|$)", q, re.I)
    if m:
        return m.group(3).strip()
    return ""


def extract_requested_attribute(q: str) -> str:
    """
    Ex: "Qui est la banque du fonds X ?" -> "banque"
    Ex: "Quel est le montant du fonds X ?" -> "montant"
    """
    q = (q or "").strip()
    m = re.search(r"^(qui\s+est|quel(le)?\s+est)\s+(l[ae]\s+)?(.+?)(\s+(du|de la|de l'|des)\s+)", q, re.I)
    if m:
        return m.group(4).strip().lower()


    m = re.search(r"^(.+?)\s+(du|de la|de l'|des)\s+", q, re.I)
    if m:
        return m.group(1).strip().lower()

    return ""


def guess_base_table(q: str) -> Optional[str]:
    ql = (q or "").lower()
    if re.search(r"\b(fonds|fond|fcpr|fcp)\b", ql):
        return "fonds"
    if re.search(r"\b(projet|pipeline)\b", ql):
        return "projet"
    if re.search(r"\b(souscription)\b", ql):
        return "souscription"
    if re.search(r"\b(lib[ée]ration|appel de fonds)\b", ql):
        return "liberation"
    return None


def _table_has_column(graph, table: str, col: str) -> bool:
    cols = graph.columns.get(table, [])
    return any(c.lower() == col.lower() for c, _dt in cols)


def _best_label_column(graph, table: str) -> Optional[str]:
    cols = [c for c, _dt in graph.columns.get(table, [])]
    lower = {c.lower(): c for c in cols}
    for cand in LABEL_COL_CANDIDATES:
        if cand in lower:
            return lower[cand]
    for c, dt in graph.columns.get(table, []):
        if dt in ("character varying", "text", "character"):
            return c
    return cols[0] if cols else None


def _find_entity_row_id(graph, table: str, name: str) -> Optional[int]:
    if not name:
        return None

    cols = [c for c, _dt in graph.columns.get(table, [])]
    name_cols = [c for c in cols if c.lower() in set(NAME_COL_CANDIDATES)]
    if not name_cols:
        return None


    where_parts = []
    params: Dict[str, Any] = {"pat": f"%{name}%"}
    for i, c in enumerate(name_cols):
        where_parts.append(f"{c} ILIKE :pat")
    where_sql = " OR ".join(where_parts)

    sql = text(f"SELECT id FROM {table} WHERE ({where_sql}) ORDER BY id LIMIT 1")
    with engine.begin() as conn:
        row = conn.execute(sql, params).first()
    return int(row[0]) if row else None


def _select_direct_column(table: str, row_id: int, col: str) -> Optional[Any]:
    sql = text(f"SELECT {col} FROM {table} WHERE id = :id")
    with engine.begin() as conn:
        row = conn.execute(sql, {"id": row_id}).first()
    return row[0] if row else None


def _select_via_fk_path(graph, base_table: str, row_id: int, target_table: str) -> Optional[Any]:
    path = shortest_path_fk(graph, base_table, target_table)
    if path is None:
        return None

    label_col = _best_label_column(graph, target_table)
    if not label_col:
        return None

    from_sql = f"{base_table} t0"
    joins = []
    where = ["t0.id = :id"]
    params = {"id": row_id}

   
    for i, edge in enumerate(path, start=1):
    
        joins.append(
            f"JOIN {edge.dst_table} t{i} ON t{i-1}.{edge.src_col} = t{i}.{edge.dst_col}"
        )

    sql = text(f"""
        SELECT t{len(path)}.{label_col}
        FROM {from_sql}
        {' '.join(joins)}
        WHERE {' AND '.join(where)}
        LIMIT 1
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, params).first()
    return row[0] if row else None


def relational_answer_one_line(question: str) -> Optional[str]:
    """
    Retourne une phrase si on sait répondre par SQL rules-first,
    sinon None (=> fallback RAG).
    """
    graph = get_schema_graph()
    base = guess_base_table(question)
    if not base or base not in graph.columns:
        return None

    entity_name = extract_entity_name(question)
    row_id = _find_entity_row_id(graph, base, entity_name)
    if row_id is None:
        return None

    attr = extract_requested_attribute(question)
    if not attr:
        return None

    
    attr_norm = attr.replace(" ", "_").replace("-", "_").lower()
    candidates = [attr_norm, attr_norm.replace("é", "e").replace("è", "e").replace("ê", "e")]

    for cand in candidates:
        if _table_has_column(graph, base, cand):
            val = _select_direct_column(base, row_id, cand)
            if val is None:
                return None
            return f"{attr.strip()} = {val}"

    target = None
    for t in graph.tables:
        if t.lower() == attr_norm or t.lower().replace("_", " ") == attr.lower():
            target = t
            break
    if target is None:
        for t in graph.tables:
            if t.lower() == (attr_norm + "s") or (t.lower().endswith("s") and t.lower()[:-1] == attr_norm):
                target = t
                break

    if not target:
        return None

    val = _select_via_fk_path(graph, base, row_id, target)
    if val is None:
        return None

    return f"{attr.strip()} = {val}"
