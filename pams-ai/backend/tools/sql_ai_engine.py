from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
from sqlalchemy import text
from app.db import engine


RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RX_LIST = re.compile(r"\b(liste|tous|toutes|affiche|donne)\b", re.I)
RX_TOTAL = re.compile(r"\b(total|somme|montant\s+total|global)\b", re.I)
RX_WHO_IS = re.compile(r"\b(qui\s+est|c'?est\s+qui)\b", re.I)
RX_OF = re.compile(r"\b(du|de\s+la|de\s+l')\b", re.I)
RX_NOMME = re.compile(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", re.I)
RX_QUOTE = re.compile(r'"([^"]{3,})"')
RX_EACH = re.compile(r"\b(chaque|par)\b", re.I)
RX_MONTANT = re.compile(r"\bmontant\b", re.I)

STOP_ATTR = {"nom", "denomination", "alias", "id"}

DEFAULT_LIST_LIMIT = 2000
HARD_LIST_LIMIT = 5000


def _extract_year(q: str) -> Optional[int]:
    m = RX_YEAR.search(q or "")
    return int(m.group(1)) if m else None


def _extract_name(q: str) -> Optional[str]:
    q = (q or "").strip()
    m = RX_QUOTE.search(q)
    if m:
        return m.group(1).strip()
    m = RX_NOMME.search(q)
    if m:
        return m.group(2).strip().strip('"').strip("'")
    return None


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("-", "_").replace(" ", "_")


@dataclass
class FKEdge:
    src_table: str
    src_col: str
    dst_table: str
    dst_col: str


@dataclass
class Schema:
    tables: Set[str]
    cols: Dict[str, List[Tuple[str, str]]]
    fks: List[FKEdge]
    pk: Dict[str, str]


_SCHEMA_CACHE: Optional[Schema] = None


def load_schema(force: bool = False) -> Schema:
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None and not force:
        return _SCHEMA_CACHE

    with engine.begin() as conn:
        tables = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type='BASE TABLE'
        """)).scalars().all()

        cols_rows = conn.execute(text("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)).mappings().all()

        pk_rows = conn.execute(text("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema='public' AND tc.constraint_type='PRIMARY KEY'
        """)).mappings().all()

        fk_rows = conn.execute(text("""
            SELECT
              tc.table_name AS src_table,
              kcu.column_name AS src_col,
              ccu.table_name AS dst_table,
              ccu.column_name AS dst_col
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema='public' AND tc.constraint_type='FOREIGN KEY'
        """)).mappings().all()

    cols: Dict[str, List[Tuple[str, str]]] = {}
    for r in cols_rows:
        cols.setdefault(r["table_name"], []).append((r["column_name"], r["data_type"]))

    pk: Dict[str, str] = {}
    for r in pk_rows:
        pk[r["table_name"]] = r["column_name"]

    fks = [FKEdge(r["src_table"], r["src_col"], r["dst_table"], r["dst_col"]) for r in fk_rows]

    _SCHEMA_CACHE = Schema(tables=set(tables), cols=cols, fks=fks, pk=pk)
    return _SCHEMA_CACHE


def _guess_label_column(schema: Schema, table: str) -> Optional[str]:
    candidates = ["denomination", "nom", "libelle", "label", "raison_sociale", "alias"]
    table_cols = {c for c, _t in schema.cols.get(table, [])}
    for c in candidates:
        if c in table_cols:
            return c
    return None


def _is_numeric_type(t: str) -> bool:
    t = (t or "").lower()
    return any(x in t for x in ["numeric", "double", "real", "integer", "bigint", "smallint", "decimal"])


def _find_table_by_keyword(schema: Schema, q: str) -> Optional[str]:
    qn = _norm(q)
    for t in schema.tables:
        if _norm(t) in qn:
            return t
    return None


def _find_year_column(schema: Schema, table: str) -> Optional[str]:
    colset = {c for c, _t in schema.cols.get(table, [])}
    for c in ["date_liberation", "date_souscription", "created_on", "updated_on", "date_lancement"]:
        if c in colset:
            return c
    for c, typ in schema.cols.get(table, []):
        if "date" in (typ or "").lower():
            return c
    return None


def _bfs_fk_path(schema: Schema, start: str, goal: str, max_hops: int = 3) -> Optional[List[FKEdge]]:
    if start == goal:
        return []
    adj: Dict[str, List[FKEdge]] = {}
    for e in schema.fks:
        adj.setdefault(e.src_table, []).append(e)
        adj.setdefault(e.dst_table, []).append(FKEdge(e.dst_table, e.dst_col, e.src_table, e.src_col))

    from collections import deque
    dq = deque([(start, [])])
    seen = {start}
    while dq:
        node, path = dq.popleft()
        if len(path) >= max_hops:
            continue
        for e in adj.get(node, []):
            nxt = e.dst_table
            if nxt in seen:
                continue
            npath = path + [e]
            if nxt == goal:
                return npath
            seen.add(nxt)
            dq.append((nxt, npath))
    return None


def _build_join_sql(schema: Schema, base_table: str, target_table: str, base_filter_sql: str, base_params: Dict[str, Any], select_expr: str) -> Tuple[str, Dict[str, Any]]:
    path = _bfs_fk_path(schema, base_table, target_table)
    if path is None:
        raise ValueError("no_fk_path")

    joins: List[str] = []
    params = dict(base_params)

    cur_alias = "t0"
    for i, e in enumerate(path, start=1):
        nxt_table = e.dst_table
        nxt_alias = f"t{i}"
        joins.append(f"LEFT JOIN {nxt_table} {nxt_alias} ON {cur_alias}.{e.src_col} = {nxt_alias}.{e.dst_col}")
        cur_alias = nxt_alias

    sql = f"""
        SELECT {select_expr} AS value
        FROM {base_table} t0
        {' '.join(joins)}
        WHERE {base_filter_sql}
        LIMIT 1
    """
    return sql, params


def try_answer_sql(message: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    schema = load_schema()
    q = message or ""
    qn = _norm(q)

    year = _extract_year(q)
    name = _extract_name(q)

    is_list = bool(RX_LIST.search(q))
    is_total = bool(RX_TOTAL.search(q))
    is_who = bool(RX_WHO_IS.search(q))

    base_table = _find_table_by_keyword(schema, q)

    if is_total:
        if not base_table:
            if "souscription" in qn:
                base_table = "souscription"
            elif "liberation" in qn or "libération" in q.lower():
                base_table = "inv_liberation_action"

        if not base_table or base_table not in schema.tables:
            return None

        num_cols = [c for c, t in schema.cols.get(base_table, []) if _is_numeric_type(t)]
        num_cols = [c for c in num_cols if "id" not in c and c not in STOP_ATTR]
        if not num_cols:
            return None

        preferred = None
        for hint in ["montant", "total", "somme"]:
            for c in num_cols:
                if hint in _norm(c):
                    preferred = c
                    break
            if preferred:
                break
        col = preferred or num_cols[0]

        where = "1=1"
        params: Dict[str, Any] = {}
        if year is not None:
            date_col = _find_year_column(schema, base_table)
            if date_col:
                where = f"EXTRACT(YEAR FROM {date_col}) = :year"
                params["year"] = int(year)

        if name and ("fonds" in qn or "fond" in qn) and base_table == "souscription":
            cols_s = {c for c, _t in schema.cols.get("souscription", [])}
            if "fonds_id" in cols_s and "fonds" in schema.tables:
                label_f = _guess_label_column(schema, "fonds") or "denomination"
                sql = text(f"""
                    SELECT COALESCE(SUM(s.{col}), 0) AS v
                    FROM souscription s
                    JOIN fonds f ON s.fonds_id = f.id
                    WHERE ({where}) AND (f.{label_f} ILIKE :fname OR COALESCE(f.alias,'') ILIKE :fname)
                """)
                params2 = dict(params)
                params2["fname"] = f"%{name}%"
                with engine.begin() as conn:
                    v = conn.execute(sql, params2).scalar_one()
                return {"ok": True, "text": f"Le total est {float(v):,.0f}".replace(",", " ") + " TND", "used": {"mode": "sql:sum", "table": "souscription", "col": col, "year": year, "filter": "fonds", "debug": debug}}

        with engine.begin() as conn:
            v = conn.execute(text(f"SELECT COALESCE(SUM({col}), 0) FROM {base_table} WHERE {where}"), params).scalar_one()

        return {"ok": True, "text": f"Le total est {float(v):,.0f}".replace(",", " ") + " TND", "used": {"mode": "sql:sum", "table": base_table, "col": col, "year": year, "debug": debug}}

    if is_who and RX_OF.search(q) and name:
        entity_table = None
        if "projet" in qn:
            entity_table = "projet"
        elif "fonds" in qn or "fond" in qn:
            entity_table = "fonds"
        if not entity_table or entity_table not in schema.tables:
            return None

        attr = q
        attr = re.sub(r"^.*?(qui\s+est|c'?est\s+qui)\s+", "", attr, flags=re.I).strip()
        attr = re.split(r"\b(du|de\s+la|de\s+l')\b", attr, flags=re.I)[0].strip()
        attr = re.sub(r"^(le|la|l')\s+", "", attr, flags=re.I).strip()
        attr_table = _norm(attr).rstrip("?")

        candidates: List[str] = []
        for t in schema.tables:
            nt = _norm(t)
            if nt == attr_table or nt.endswith("_" + attr_table) or attr_table in nt:
                candidates.append(t)

        if not candidates:
            fk_cols = {c for c, _t in schema.cols.get(entity_table, []) if c.endswith("_id")}
            for c in fk_cols:
                if attr_table in _norm(c.replace("_id", "")):
                    for e in schema.fks:
                        if e.src_table == entity_table and e.src_col == c:
                            candidates.append(e.dst_table)

        if not candidates:
            return None

        target_table = candidates[0]
        base_label = _guess_label_column(schema, entity_table) or "nom"
        target_label = _guess_label_column(schema, target_table) or schema.pk.get(target_table)
        if not target_label:
            return None

        base_filter = f"(t0.{base_label} ILIKE :name)"
        params = {"name": f"%{name}%"}
        path = _bfs_fk_path(schema, entity_table, target_table)
        if path is None:
            return None

        sql, params = _build_join_sql(schema, entity_table, target_table, base_filter, params, f"t{len(path)}.{target_label}")

        with engine.begin() as conn:
            row = conn.execute(text(sql), params).mappings().first()

        if not row or row.get("value") is None:
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:rel", "hit": False, "debug": debug}}

        return {"ok": True, "text": str(row["value"]), "used": {"mode": "sql:rel", "entity": entity_table, "attr": target_table, "debug": debug}}

    if is_list:
        if not base_table:
            if "fonds" in qn or "fond" in qn:
                base_table = "fonds"
            elif "projet" in qn or "projects" in qn:
                base_table = "projet"

        if not base_table or base_table not in schema.tables:
            return None

        label_col = _guess_label_column(schema, base_table) or schema.pk.get(base_table)
        if not label_col:
            return None

        want_amounts = bool(RX_MONTANT.search(q) and RX_EACH.search(q)) or (base_table == "fonds" and RX_MONTANT.search(q))

        qlow = (q or "").lower()
        unlimited = any(w in qlow for w in ["tout", "tous", "toutes"])
        limit = HARD_LIST_LIMIT if unlimited else DEFAULT_LIST_LIMIT

        if want_amounts and base_table == "fonds" and "souscription" in schema.tables:
            cols_s = {c for c, _t in schema.cols.get("souscription", [])}
            if "fonds_id" in cols_s:
                montant_col = None
                for c, t in schema.cols.get("souscription", []):
                    if c in ("montant_souscription", "montant"):
                        if _is_numeric_type(t):
                            montant_col = c
                            break
                if montant_col:
                    where = "1=1"
                    params: Dict[str, Any] = {}
                    if year is not None:
                        date_col = _find_year_column(schema, "souscription")
                        if date_col:
                            where = f"EXTRACT(YEAR FROM s.{date_col}) = :year"
                            params["year"] = int(year)

                    with engine.begin() as conn:
                        rows = conn.execute(text(f"""
                            SELECT f.{label_col} AS label, COALESCE(SUM(s.{montant_col}),0) AS amount
                            FROM fonds f
                            LEFT JOIN souscription s ON s.fonds_id = f.id
                            WHERE {where}
                            GROUP BY f.{label_col}
                            ORDER BY f.{label_col} ASC
                            LIMIT :limit
                        """), {**params, "limit": limit}).mappings().all()

                    if not rows:
                        return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:list_amounts", "table": "fonds", "from": "souscription", "debug": debug}}

                    out = "Voici la liste :\n\n" + "\n".join(
                        [f"{i+1}. {str(r['label'])} : {float(r['amount'] or 0):,.0f}".replace(",", " ") + " TND" for i, r in enumerate(rows)]
                    )
                    return {"ok": True, "text": out, "used": {"mode": "sql:list_amounts", "table": "fonds", "from": "souscription", "col": montant_col, "year": year, "count": len(rows), "debug": debug}}

        with engine.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT {label_col} AS v
                FROM {base_table}
                ORDER BY {label_col} ASC
                LIMIT :limit
            """), {"limit": limit}).scalars().all()

        if not rows:
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:list", "table": base_table, "debug": debug}}

        out = "Voici la liste :\n\n" + "\n".join([f"{i+1}. {str(v)}" for i, v in enumerate(rows)])
        return {"ok": True, "text": out, "used": {"mode": "sql:list", "table": base_table, "count": len(rows), "debug": debug}}

    return None
