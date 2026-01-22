from __future__ import annotations

import re
from typing import Any, Dict, Optional

from tools.analytics_parser import parse_intent, extract_entity_name
from tools.analytics_sql import run_sql_scalar, run_sql_one, build_fk_lookup_sql

RX_INVESTI = re.compile(r"\b(investi|investis|investissement)\b", re.I)
RX_ACTIF = re.compile(r"\b(actif|actifs)\b", re.I)
RX_TOTAL = re.compile(r"\b(total|somme|global|montant\s+total)\b", re.I)

RX_ACTIONS = re.compile(r"\b(action|actions)\b", re.I)
RX_OCA = re.compile(r"\b(oca)\b", re.I)
RX_CCA = re.compile(r"\b(cca)\b", re.I)

RX_FUND_WORD = re.compile(r"\b(fond|fonds)\b", re.I)


METRIC_SHORTCUTS: Dict[str, Dict[str, Any]] = {
    "total_investi": {
        "sql": """
            SELECT
              COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_action {w}), 0)
            + COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_oca {w}), 0)
            + COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_cca {w}), 0)
        """,
        "unit": "TND",
        "label": "Le total est",
        "date_col": "date_liberation",
    },
    "investi_actions": {
        "sql": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_action {w}",
        "unit": "TND",
        "label": "Le total est",
        "date_col": "date_liberation",
    },
    "investi_oca": {
        "sql": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_oca {w}",
        "unit": "TND",
        "label": "Le total est",
        "date_col": "date_liberation",
    },
    "investi_cca": {
        "sql": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_cca {w}",
        "unit": "TND",
        "label": "Le total est",
        "date_col": "date_liberation",
    },
    "total_actif": {
        "sql": """
            SELECT COALESCE(SUM(s.montant_souscription), 0)
            FROM souscription s
            JOIN fonds f ON f.id = s.fonds_id
            {w}
        """,
        "unit": "TND",
        "label": "Le total est",
        "date_col": "s.date_souscription",
        "fund_col": "f.denomination",
    }
}


def _format_money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def _metric_from_message(msg: str) -> Optional[str]:
    q = msg or ""

    if RX_INVESTI.search(q):
        if RX_ACTIONS.search(q):
            return "investi_actions"
        if RX_OCA.search(q):
            return "investi_oca"
        if RX_CCA.search(q):
            return "investi_cca"
        if RX_TOTAL.search(q):
            return "total_investi"
        return "total_investi"

    if RX_ACTIF.search(q) and RX_TOTAL.search(q):
        return "total_actif"
    if RX_ACTIF.search(q) and not RX_INVESTI.search(q):
        return "total_actif"

    return None


def run_analytics(message: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    q = message or ""
    intent = parse_intent(q)

    metric = _metric_from_message(q)
    if metric:
        return _run_metric(metric, q, debug=debug)

    if not intent:
        return None

    if intent.kind == "rel":
        if not intent.entity_table or not intent.attr_table:
            return None
        name = intent.entity_name or extract_entity_name(q)
        if not name:
            return None
        built = build_fk_lookup_sql(intent.entity_table, intent.attr_table, name)
        if not built:
            return None
        sql, params = built
        row = run_sql_one(sql, params)
        if not row or row.get("value") is None:
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "analytics:rel", "hit": False}}
        return {
            "ok": True,
            "text": str(row["value"]),
            "used": {"mode": "analytics:rel", "entity_table": intent.entity_table, "attr_table": intent.attr_table, "debug": debug},
        }

    if intent.kind == "agg":
        metric = _metric_from_message(q)
        if metric:
            return _run_metric(metric, q, debug=debug)
        return None

    return None


def _run_metric(metric: str, message: str, debug: bool = False) -> Dict[str, Any]:
    intent = parse_intent(message)
    year = intent.year if intent else None
    fund_name = intent.fund_name if intent else None

    m = METRIC_SHORTCUTS.get(metric)
    if not m:
        return {"ok": False, "text": "Je ne sais pas.", "used": {"mode": "analytics:metric", "metric": metric}}

    clauses = []
    params: Dict[str, Any] = {}

    if year is not None:
        clauses.append(f"EXTRACT(YEAR FROM {m['date_col']}) = :year")
        params["year"] = int(year)

    if fund_name and RX_FUND_WORD.search(message or "") and m.get("fund_col"):
        clauses.append(f"{m['fund_col']} ILIKE :fund")
        params["fund"] = f"%{fund_name}%"

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)

    sql = m["sql"].format(w=where)
    val = run_sql_scalar(sql, params)

    text = f"{m['label']} {_format_money(val)} {m['unit']}"
    return {
        "ok": True,
        "text": text,
        "used": {"mode": "analytics:metric", "metric": metric, "year": year, "fund": fund_name or "", "debug": debug},
    }
