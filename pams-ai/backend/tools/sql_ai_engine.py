from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
from sqlalchemy import text
from app.db import engine

RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RX_LIST = re.compile(r"\b(liste|tous|toutes)\b", re.I)
RX_TOTAL = re.compile(r"\b(total|somme|montant\s+total|global)\b", re.I)
RX_COUNT = re.compile(r"\b(nombre|combien)\b", re.I)

RX_REPARTITION = re.compile(r"\b(r[eé]partition|repartition|distribution)\b", re.I)
RX_BY_STATE = re.compile(r"\b(par\s+(?:[eé]tat|etat|statut|status))\b", re.I)
RX_BY_YEAR = re.compile(r"\b(par\s+(?:ann[eé]e|annee|an))\b", re.I)
RX_STATUT = re.compile(r"\b(statut|[eé]tat|etat|avancement)\b", re.I)

RX_WHO_IS = re.compile(r"\b(qui\s+est|c'?est\s+qui)\b", re.I)
RX_OF = re.compile(r"\b(du|de\s+la|de\s+l')\b", re.I)
RX_NOMME = re.compile(r"\b(nomm[eé]|appel[eé])\s+(.+?)(\?|$)", re.I)
RX_QUOTE = re.compile(r'"([^"]{3,})"')
RX_EACH = re.compile(r"\b(chaque|par)\b", re.I)
RX_MONTANT = re.compile(r"\bmontant\b", re.I)

STOP_ATTR = {"nom", "denomination", "alias", "id"}

DEFAULT_LIST_LIMIT = 1000
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
    candidates = ["libelle", "label", "nom", "denomination", "raison_sociale", "alias"]
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
    for c in ["created_on", "updated_on", "date_creation", "date_debut", "date_fin", "date_lancement", "date"]:
        if c in colset:
            return c
    for c, typ in schema.cols.get(table, []):
        if "date" in (typ or "").lower():
            return c
    return None


def _pick_amount_column(schema: Schema, table: str) -> Optional[str]:
    table_cols = {c for c, _t in schema.cols.get(table, [])}
    for c in ["montant", "total", "montant_souscription", "montant_liberation"]:
        if c in table_cols:
            return c
    return None


def _find_fk(schema: Schema, src_table: str, dst_table: str) -> Optional[FKEdge]:
    for e in schema.fks:
        if e.src_table == src_table and e.dst_table == dst_table:
            return e
    return None


def _resolve_join_group(
    schema: Schema,
    base_table: str,
    state_table: str,
    base_alias: str,
    state_alias: str,
    fallback_fk_cols: List[str],
) -> Optional[Tuple[str, str]]:
    if base_table not in schema.tables or state_table not in schema.tables:
        return None

    fk = _find_fk(schema, base_table, state_table)
    if fk:
        join_sql = f"LEFT JOIN {state_table} {state_alias} ON {base_alias}.{fk.src_col} = {state_alias}.{fk.dst_col}"
    else:
        base_cols = {c for c, _t in schema.cols.get(base_table, [])}
        dst_pk = schema.pk.get(state_table) or "id"
        fk_col = next((c for c in fallback_fk_cols if c in base_cols), None)
        if not fk_col:
            return None
        join_sql = f"LEFT JOIN {state_table} {state_alias} ON {base_alias}.{fk_col} = {state_alias}.{dst_pk}"

    label = _guess_label_column(schema, state_table) or schema.pk.get(state_table)
    if not label:
        return None

    group_expr = f"COALESCE({state_alias}.{label}::text, 'N/A')"
    return join_sql, group_expr


def _fmt_year(v: Any) -> str:
    try:
        iv = int(v)
    except Exception:
        return "N/A"
    return "N/A" if iv == -1 else str(iv)


def _safe_query(sql: Any, params: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    try:
        with engine.begin() as conn:
            return conn.execute(sql, params).mappings().all()
    except Exception:
        return None


def try_answer_sql(message: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    schema = load_schema()
    q = message or ""
    qn = _norm(q)

    year = _extract_year(q)
    name = _extract_name(q)

    is_list = bool(RX_LIST.search(q))
    is_total = bool(RX_TOTAL.search(q))
    is_count = bool(RX_COUNT.search(q))
    is_repartition = bool(RX_REPARTITION.search(q)) and (RX_BY_STATE.search(q) or RX_STATUT.search(q))
    by_year = bool(RX_BY_YEAR.search(q))

    base_table = _find_table_by_keyword(schema, q)

    if is_repartition:
        if not base_table:
            if "projet" in qn or "projects" in qn:
                base_table = "projet"
            elif "fonds" in qn or "fond" in qn:
                base_table = "fonds"
            elif "souscription" in qn:
                base_table = "souscription"
            elif "liberation" in qn or "libération" in q.lower():
                base_table = "liberation"

        if not base_table or base_table not in schema.tables:
            return None

        params: Dict[str, Any] = {}
        where = "1=1"

        if base_table == "projet":
            resolved = _resolve_join_group(
                schema=schema,
                base_table="projet",
                state_table="etat_avancement",
                base_alias="p",
                state_alias="ea",
                fallback_fk_cols=["etat_id", "ett_id"],
            )
            if not resolved:
                return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:group", "table": "projet", "debug": debug}}

            join_sql, statut_expr = resolved
            amount_col = _pick_amount_column(schema, "projet")

            date_col = _find_year_column(schema, "projet")
            if by_year and not date_col:
                return {
                    "ok": True,
                    "text": "Impossible de calculer 'par année' : aucune colonne date détectée dans la table projet.",
                    "used": {"mode": "sql:group", "table": "projet", "group": "etat_avancement", "year": year, "debug": debug},
                }

            year_expr = f"EXTRACT(YEAR FROM p.{date_col})::int" if date_col else None
            year_expr_safe = f"COALESCE({year_expr}, -1)" if year_expr else None

            if year is not None and date_col:
                where = f"EXTRACT(YEAR FROM p.{date_col}) = :year"
                params["year"] = int(year)

            if by_year and year_expr_safe:
                if amount_col:
                    sql = text(f"""
                        SELECT {year_expr_safe} AS annee,
                               {statut_expr} AS statut,
                               COUNT(*)::bigint AS n,
                               COALESCE(SUM(p.{amount_col}), 0) AS s
                        FROM projet p
                        {join_sql}
                        WHERE {where}
                        GROUP BY {year_expr_safe}, {statut_expr}
                        ORDER BY {year_expr_safe}, {statut_expr}
                    """)
                else:
                    sql = text(f"""
                        SELECT {year_expr_safe} AS annee,
                               {statut_expr} AS statut,
                               COUNT(*)::bigint AS n
                        FROM projet p
                        {join_sql}
                        WHERE {where}
                        GROUP BY {year_expr_safe}, {statut_expr}
                        ORDER BY {year_expr_safe}, {statut_expr}
                    """)
            else:
                if amount_col:
                    sql = text(f"""
                        SELECT {statut_expr} AS statut,
                               COUNT(*)::bigint AS n,
                               COALESCE(SUM(p.{amount_col}), 0) AS s
                        FROM projet p
                        {join_sql}
                        WHERE {where}
                        GROUP BY {statut_expr}
                        ORDER BY {statut_expr}
                    """)
                else:
                    sql = text(f"""
                        SELECT {statut_expr} AS statut,
                               COUNT(*)::bigint AS n
                        FROM projet p
                        {join_sql}
                        WHERE {where}
                        GROUP BY {statut_expr}
                        ORDER BY {statut_expr}
                    """)

            rows = _safe_query(sql, params)
            if not rows:
                return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:group", "table": "projet", "debug": debug}}

            lines: List[str] = []
            if by_year and year_expr_safe:
                lines.append("Répartition des projets par année et statut :\n")
                if amount_col:
                    for r in rows:
                        lines.append(
                            f"- {_fmt_year(r.get('annee'))} | {r.get('statut')} : {int(r['n'])} "
                            f"(montant: {float(r['s']):,.0f}".replace(",", " ") + " TND)"
                        )
                else:
                    for r in rows:
                        lines.append(f"- {_fmt_year(r.get('annee'))} | {r.get('statut')} : {int(r['n'])}")
            else:
                lines.append("Répartition des projets par statut :\n")
                if amount_col:
                    for r in rows:
                        lines.append(
                            f"- {r.get('statut')} : {int(r['n'])} "
                            f"(montant: {float(r['s']):,.0f}".replace(",", " ") + " TND)"
                        )
                else:
                    for r in rows:
                        lines.append(f"- {r.get('statut')} : {int(r['n'])}")

            return {"ok": True, "text": "\n".join(lines), "used": {"mode": "sql:group", "table": "projet", "group": "etat_avancement", "year": year, "debug": debug}}

        if base_table == "fonds":
            resolved = _resolve_join_group(
                schema=schema,
                base_table="fonds",
                state_table="etat_fonds",
                base_alias="f",
                state_alias="ef",
                fallback_fk_cols=["etat_id", "etat_fonds_id"],
            )
            if not resolved:
                return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:group", "table": "fonds", "debug": debug}}

            join_sql, etat_expr = resolved
            amount_col = _pick_amount_column(schema, "fonds")

            if amount_col:
                sql = text(f"""
                    SELECT {etat_expr} AS etat,
                           COUNT(*)::bigint AS n,
                           COALESCE(SUM(f.{amount_col}), 0) AS s
                    FROM fonds f
                    {join_sql}
                    WHERE 1=1
                    GROUP BY {etat_expr}
                    ORDER BY {etat_expr}
                """)
            else:
                sql = text(f"""
                    SELECT {etat_expr} AS etat,
                           COUNT(*)::bigint AS n
                    FROM fonds f
                    {join_sql}
                    WHERE 1=1
                    GROUP BY {etat_expr}
                    ORDER BY {etat_expr}
                """)

            rows = _safe_query(sql, {})
            if not rows:
                return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:group", "table": "fonds", "debug": debug}}

            lines: List[str] = []
            lines.append("Répartition des fonds par état :\n")
            if amount_col:
                for r in rows:
                    lines.append(f"- {r.get('etat')} : {int(r['n'])} (montant: {float(r['s']):,.0f}".replace(",", " ") + " TND)")
            else:
                for r in rows:
                    lines.append(f"- {r.get('etat')} : {int(r['n'])}")

            return {"ok": True, "text": "\n".join(lines), "used": {"mode": "sql:group", "table": "fonds", "group": "etat_fonds", "debug": debug}}

        return None

    if is_list:
        if name and ("fonds" in qn or "fond" in qn):
            return None

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

        table_cols = {c for c, _t in schema.cols.get(base_table, [])}
        want_amounts = bool(RX_MONTANT.search(q) and RX_EACH.search(q)) or (base_table == "fonds" and RX_MONTANT.search(q))
        amount_col = None
        if want_amounts:
            for c in ["montant", "montant_souscription", "montant_liberation"]:
                if c in table_cols:
                    amount_col = c
                    break

        qlow = (q or "").lower()
        unlimited = any(w in qlow for w in ["tout", "tous", "toutes"])
        limit = HARD_LIST_LIMIT if unlimited else DEFAULT_LIST_LIMIT

        try:
            with engine.begin() as conn:
                if want_amounts and amount_col:
                    rows = conn.execute(
                        text(f"""
                            SELECT {label_col} AS label, {amount_col} AS amount
                            FROM {base_table}
                            ORDER BY {label_col} ASC
                            LIMIT :limit
                        """),
                        {"limit": limit},
                    ).mappings().all()

                    if not rows:
                        return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:list_amounts", "table": base_table, "debug": debug}}

                    out = "Voici la liste :\n\n" + "\n".join(
                        [f"{i+1}. {str(r['label'])} : {float(r['amount'] or 0):,.0f}".replace(",", " ") + " TND" for i, r in enumerate(rows)]
                    )
                    return {"ok": True, "text": out, "used": {"mode": "sql:list_amounts", "table": base_table, "count": len(rows), "col": amount_col, "debug": debug}}

                rows = conn.execute(
                    text(f"""
                        SELECT {label_col} AS v
                        FROM {base_table}
                        ORDER BY {label_col} ASC
                        LIMIT :limit
                    """),
                    {"limit": limit},
                ).scalars().all()
        except Exception:
            return None

        if not rows:
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "sql:list", "table": base_table, "debug": debug}}

        out = "Voici la liste :\n\n" + "\n".join([f"{i+1}. {str(v)}" for i, v in enumerate(rows)])
        return {"ok": True, "text": out, "used": {"mode": "sql:list", "table": base_table, "count": len(rows), "debug": debug}}

    if is_count:
        if not base_table:
            if "souscription" in qn:
                base_table = "souscription"
            elif "liberation" in qn or "libération" in q.lower():
                base_table = "liberation"
            elif "fonds" in qn or "fond" in qn:
                base_table = "fonds"
            elif "projet" in qn:
                base_table = "projet"

        if not base_table or base_table not in schema.tables:
            return None

        where = "1=1"
        params: Dict[str, Any] = {}
        if year is not None:
            date_col = _find_year_column(schema, base_table)
            if date_col:
                where = f"EXTRACT(YEAR FROM {date_col}) = :year"
                params["year"] = int(year)

        try:
            with engine.begin() as conn:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {base_table} WHERE {where}"), params).scalar_one()
        except Exception:
            return None

        return {"ok": True, "text": f"Le nombre est {int(n)}.", "used": {"mode": "sql:count", "table": base_table, "year": year, "debug": debug}}

    if is_total:
        if not base_table:
            if "souscription" in qn:
                base_table = "souscription"
            elif "liberation" in qn or "libération" in q.lower():
                base_table = "liberation"

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

        try:
            with engine.begin() as conn:
                v = conn.execute(text(f"SELECT COALESCE(SUM({col}), 0) FROM {base_table} WHERE {where}"), params).scalar_one()
        except Exception:
            return None

        return {"ok": True, "text": f"Le total est {float(v):,.0f}".replace(",", " ") + " TND", "used": {"mode": "sql:sum", "table": base_table, "col": col, "year": year, "debug": debug}}

    if bool(RX_WHO_IS.search(q)) and RX_OF.search(q) and name:
        return None

    return None
