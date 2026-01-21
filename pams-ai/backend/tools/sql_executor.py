from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
from sqlalchemy import text

from app.db import engine
from tools.schema_graph import load_schema


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _apply_year_filter(where: str, date_col: str) -> Tuple[str, Dict[str, Any]]:
    """
    Construit un filtre YEAR(date_col)=:year de façon portable PG.
    """
    params: Dict[str, Any] = {}
    if not date_col:
        return where, params
    clause = f"EXTRACT(YEAR FROM {_quote_ident(date_col)}) = :year"
    if where:
        where = where + " AND " + clause
    else:
        where = clause
    return where, params


def run_analytics_sql(plan, debug: bool = False) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Execute un plan AnalyticsPlan.
    Retour: (value, used_meta)
    """
    sg = load_schema(engine)

    used = {"engine": "sql-rules", "debug": bool(debug)}

    if plan.kind == "single":
        table = plan.table
        col = plan.column
        date_col = plan.date_col

        if not table or not col:
            return None, {**used, "error": "missing_table_or_column"}

        if not sg.has_table(table) or not sg.has_column(table, col):
            return None, {**used, "error": "unknown_table_or_column", "table": table, "column": col}

        if plan.year and date_col and (not sg.has_column(table, date_col)):
            return None, {**used, "error": "missing_date_col", "table": table, "date_col": date_col}

        agg_sql = {
            "sum": "COALESCE(SUM({col}), 0)",
            "avg": "COALESCE(AVG({col}), 0)",
            "min": "COALESCE(MIN({col}), 0)",
            "max": "COALESCE(MAX({col}), 0)",
        }.get(plan.op)

        if plan.op == "count":
            expr = "COUNT(*)"
        elif agg_sql:
            expr = agg_sql.format(col=_quote_ident(col))
        else:
            return None, {**used, "error": "unsupported_op", "op": plan.op}

        where = ""
        params: Dict[str, Any] = {}
        if plan.year and date_col:
            where, _ = _apply_year_filter(where, date_col)
            params["year"] = plan.year

        sql = f"SELECT {expr} AS value FROM {_quote_ident(table)}"
        if where:
            sql += f" WHERE {where}"

        used.update({"sql_mode": "single", "table": table, "column": col, "year": plan.year})

        with engine.begin() as conn:
            row = conn.execute(text(sql), params).first()
            val = float(row[0]) if row and row[0] is not None else 0.0
        return val, used

    if plan.kind == "multi_sum":
        targets = plan.targets or []
        if not targets:
            return None, {**used, "error": "empty_targets"}

        total = 0.0
        for t in targets:
            table = t.get("table")
            col = t.get("column")
            date_col = t.get("date_col")

            if not table or not col:
                continue
            if not sg.has_table(table) or not sg.has_column(table, col):
                continue
            if plan.year and date_col and (not sg.has_column(table, date_col)):
                continue

            where = ""
            params: Dict[str, Any] = {}
            if plan.year and date_col:
                where, _ = _apply_year_filter(where, date_col)
                params["year"] = plan.year

            sql = f"SELECT COALESCE(SUM({_quote_ident(col)}),0) AS value FROM {_quote_ident(table)}"
            if where:
                sql += f" WHERE {where}"

            with engine.begin() as conn:
                row = conn.execute(text(sql), params).first()
                v = float(row[0]) if row and row[0] is not None else 0.0
            total += v

        used.update({"sql_mode": "multi_sum", "year": plan.year, "targets": [t.get("table") for t in targets]})
        return total, used

    return None, {**used, "error": "unsupported_kind", "kind": plan.kind}
