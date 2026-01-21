from __future__ import annotations

import re
from typing import Any, Dict, Optional

from sqlalchemy import text

from tools.analytics_parser import parse_intent, extract_entity_name
from tools.analytics_sql import run_sql_scalar, run_sql_one, build_fk_lookup_sql


METRIC_SHORTCUTS = {
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
    "total_actif": {
        "sql": "SELECT COALESCE(SUM(montant_souscription), 0) FROM souscription {w}",
        "unit": "TND",
        "label": "Le total est",
        "date_col": "date_souscription",
    }
}

RX_INVESTI = re.compile(r"\b(total\s+investi|investi)\b", re.I)
RX_ACTIF = re.compile(r"\b(total\s+actif|actif)\b", re.I)
RX_SOUSCRIPTION = re.compile(r"\b(souscription|souscriptions)\b", re.I)
RX_MONTANT = re.compile(r"\b(montant|valeur)\b", re.I)

def _format_money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")

def _metric_from_message(msg: str) -> Optional[str]:
    q = msg or ""
    if RX_INVESTI.search(q) and re.search(r"\btotal\b", q, re.I):
        return "total_investi"
    if RX_ACTIF.search(q) and re.search(r"\btotal\b", q, re.I):
        return "total_actif"
    return None

def run_analytics(message: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    q = message or ""
    intent = parse_intent(q)

    if not intent:
        metric = _metric_from_message(q)
        if metric:
            return _run_metric(metric, q, debug=debug)
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
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "analytics:rel", "hit": False, "debug": debug}}
        return {"ok": True, "text": str(row["value"]), "used": {"mode": "analytics:rel", "entity_table": intent.entity_table, "attr_table": intent.attr_table, "debug": debug}}

    if intent.kind == "agg":
        metric = _metric_from_message(q)
        if metric:
            return _run_metric(metric, q, debug=debug)

        if intent.agg == "sum" and intent.target_table == "souscription" and RX_SOUSCRIPTION.search(q) and RX_MONTANT.search(q):
            fund_name = intent.entity_name or extract_entity_name(q)
            year = intent.year
            if not fund_name:
                return None

            where = []
            params: Dict[str, Any] = {"fund": f"%{fund_name}%"}
            where.append("(f.denomination ILIKE :fund OR f.alias ILIKE :fund)")

            if year is not None:
                where.append("EXTRACT(YEAR FROM s.date_souscription) = :year")
                params["year"] = int(year)

            sql = f"""
                SELECT COALESCE(SUM(s.montant_souscription), 0)
                FROM souscription s
                JOIN fonds f ON f.id = s.fonds_id
                WHERE {" AND ".join(where)}
            """
            val = run_sql_scalar(sql, params)
            return {
                "ok": True,
                "text": f"Le total est {_format_money(val)} TND",
                "used": {"mode": "analytics:agg", "target": "souscription.montant_souscription", "fund": fund_name, "year": year, "debug": debug},
            }

        return None

    return None

def _run_metric(metric: str, message: str, debug: bool = False) -> Dict[str, Any]:
    intent = parse_intent(message)
    year = intent.year if intent else None

    m = METRIC_SHORTCUTS.get(metric)
    if not m:
        return {"ok": False, "text": "Je ne sais pas.", "used": {"mode": "analytics:metric", "metric": metric, "debug": debug}}

    where = ""
    params: Dict[str, Any] = {}
    if year is not None:
        where = f"WHERE EXTRACT(YEAR FROM {m['date_col']}) = :year"
        params["year"] = int(year)

    sql = m["sql"].format(w=(" " + where if where else ""))
    val = run_sql_scalar(sql, params)

    return {"ok": True, "text": f"{m['label']} {_format_money(val)} {m['unit']}", "used": {"mode": "analytics:metric", "metric": metric, "year": year, "debug": debug}}
