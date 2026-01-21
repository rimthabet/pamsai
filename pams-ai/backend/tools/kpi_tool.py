# tools/kpi_tool.py
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from tools.schema_catalog import SchemaCatalog, MAXULA_DB_URL
from tools.join_resolver import JoinResolver, JoinPlan


MAX_GROUP_ROWS = int(os.getenv("KPI_MAX_GROUP_ROWS", "200"))
MAX_HOPS = int(os.getenv("KPI_MAX_JOIN_HOPS", "3"))

AGG_RULES = [
    ("sum", re.compile(r"\b(total|somme|montant total|total des|total du|somme des)\b", re.I)),
    ("avg", re.compile(r"\b(moyenne|avg)\b", re.I)),
    ("count", re.compile(r"\b(nombre|combien|count|nb)\b", re.I)),
]


DOMAIN_TABLE_HINTS = [
    ("fonds", ["fonds"]),
    ("projet", ["projet", "projects", "projets"]),
    ("souscription", ["souscription", "souscriptions"]),
    ("liberation", ["liberation", "liberations"]),
]


METRIC_HINTS = [
    ("montant", ["montant", "amount"]),
    ("capital", ["capital", "capital_social"]),
    ("investi", ["investi", "investissement", "montant_investi"]),
    ("engagement", ["engagement"]),
]

DIM_HINTS = [
    ("banque", ["banque"]),
    ("etat", ["etat", "status", "statut"]),
    ("secteur", ["secteur", "activite", "activité"]),
    ("type", ["type"]),
]

class KpiTool:
    def __init__(self, engine: Optional[Engine] = None, catalog: Optional[SchemaCatalog] = None):
        self.engine = engine or create_engine(MAXULA_DB_URL, pool_pre_ping=True)
        self.catalog = catalog or SchemaCatalog(self.engine)
        self.resolver = JoinResolver(self.catalog)

    def _detect_agg(self, q: str) -> str:
        for agg, rx in AGG_RULES:
            if rx.search(q):
                return agg
        
        return "sum" if re.search(r"\b(total)\b", q, re.I) else "count"

    def _pick_base_table(self, q: str) -> Optional[str]:
        ql = (q or "").lower()
        
        for _, candidates in DOMAIN_TABLE_HINTS:
            for t in candidates:
                if re.search(rf"\b{re.escape(t)}\b", ql):
                    
                    if t in self.catalog.tables:
                        return t
                    
                    for real in self.catalog.tables.keys():
                        if real.lower() == t.lower():
                            return real

        
        for t in self.catalog.tables.keys():
            if re.search(rf"\b{re.escape(t.lower())}\b", ql):
                return t

        return None

    def _pick_metric_col(self, base_table: str, q: str, agg: str) -> Optional[str]:
        info = self.catalog.tables[base_table]
        if agg == "count":
            return None

        ql = (q or "").lower()

        
        for _, col_candidates in METRIC_HINTS:
            if any(k in ql for k in col_candidates):
                for c in info.numeric_cols:
                    if any(k in c.lower() for k in col_candidates):
                        return c

        
        for c in info.numeric_cols:
            if c.lower() == "montant":
                return c

        
        return info.numeric_cols[0] if info.numeric_cols else None

    def _pick_dimension_table(self, q: str) -> Optional[str]:
        ql = (q or "").lower()
        for _, dims in DIM_HINTS:
            if any(d in ql for d in dims):
                for t in self.catalog.tables.keys():
                    if any(d == t.lower() or d in t.lower() for d in dims):
                        return t
        return None

    def _build_sql(self, base_table: str, agg: str, metric_col: Optional[str],
                   dim_table: Optional[str]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        info = self.catalog.tables[base_table]

        joins_sql = ""
        select_dim = ""
        group_by = ""
        order_by = ""
        meta: Dict[str, Any] = {"base_table": base_table, "agg": agg}

        
        dim_label_expr = None
        if dim_table:
            jp: Optional[JoinPlan] = self.resolver.find_join_path(base_table, dim_table, max_hops=MAX_HOPS)
            if jp and jp.steps:
                for s in jp.steps:
                    joins_sql += f'\nLEFT JOIN "{s.right_table}" ON "{s.left_table}"."{s.left_col}" = "{s.right_table}"."{s.right_col}"'
                label_col = jp.target_label_col
                if label_col:
                    dim_label_expr = f'"{dim_table}"."{label_col}"'
                else:
                    tinfo = self.catalog.tables[dim_table]
                    if tinfo.pk_cols:
                        dim_label_expr = f'"{dim_table}"."{tinfo.pk_cols[0]}"'
                    elif tinfo.text_cols:
                        dim_label_expr = f'"{dim_table}"."{tinfo.text_cols[0]}"'
                    else:
                        dim_label_expr = f'"{dim_table}"."{list(tinfo.columns.keys())[0]}"'

                select_dim = f"{dim_label_expr} AS dimension"
                group_by = f"\nGROUP BY {dim_label_expr}"
                order_by = "\nORDER BY value DESC NULLS LAST"
                meta["dimension"] = {"table": dim_table, "label_expr": dim_label_expr, "hops": len(jp.steps)}
            else:
                
                meta["dimension"] = {"table": dim_table, "error": "no_join_path"}
                dim_table = None

        
        if agg == "count":
            value_expr = "COUNT(*)::bigint"
        else:
            if not metric_col:
                value_expr = "COUNT(*)::bigint"
                meta["metric_fallback"] = "count_no_metric"
            else:
                value_expr = f'{agg.upper()}("{base_table}"."{metric_col}")'

        select_parts = []
        if select_dim:
            select_parts.append(select_dim)
        select_parts.append(f"{value_expr} AS value")

        sql = f'''
SELECT
  {", ".join(select_parts)}
FROM "{base_table}"
{joins_sql}
{group_by}
{order_by}
LIMIT {MAX_GROUP_ROWS if dim_table else 1}
'''.strip()

        return sql, {}, meta

    def run(self, question: str) -> Dict[str, Any]:
        q = question or ""
        agg = self._detect_agg(q)

        base_table = self._pick_base_table(q)
        if not base_table:
            return {
                "ok": False,
                "error": "cannot_detect_base_table",
                "message": "Je peux calculer des KPI, mais je n’arrive pas à identifier la table cible (fonds/projet/souscription/libération…).",
                "plan": {"agg": agg},
            }

        metric_col = self._pick_metric_col(base_table, q, agg)
        dim_table = self._pick_dimension_table(q)

        sql, params, meta = self._build_sql(base_table, agg, metric_col, dim_table)

        with self.engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

    
        out_rows = [dict(r) for r in rows]
        return {
            "ok": True,
            "sql": sql,
            "rows": out_rows,
            "meta": {
                **meta,
                "metric_col": metric_col,
                "dim_table": dim_table,
                "row_count": len(out_rows),
            }
        }
