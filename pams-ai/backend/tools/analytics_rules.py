import re
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple

# Agrégations
RX_SUM = re.compile(r"\b(total|somme|global|montant total|total des)\b", re.I)
RX_AVG = re.compile(r"\b(moyenne)\b", re.I)
RX_COUNT = re.compile(r"\b(nombre|combien|count)\b", re.I)
RX_MIN = re.compile(r"\b(minimum|min)\b", re.I)
RX_MAX = re.compile(r"\b(maximum|max)\b", re.I)

# Group by
RX_BY = re.compile(r"\b(par|par\s+année|par\s+fonds|par\s+projet)\b", re.I)

# Année 
RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

# KPI synonymes
SYN_METRICS: List[Tuple[re.Pattern, Dict[str, Any]]] = [
    (re.compile(r"\b(total\s+investi|investi\s+total)\b", re.I),
     {"op": "sum", "kind": "multi_sum", "targets": [
         {"table": "inv_liberation_action", "column": "montant_liberation", "date_col": "date_liberation"},
         {"table": "inv_liberation_oca", "column": "montant_liberation", "date_col": "date_liberation"},
         {"table": "inv_liberation_cca", "column": "montant_liberation", "date_col": "date_liberation"},
     ]}),

  
    (re.compile(r"\b(total\s+actif|actif\s+total)\b", re.I),
     {"op": "sum", "kind": "single", "table": "souscription", "column": "montant_souscription", "date_col": "date_souscription"}),

    
    (re.compile(r"\b(montant\s+total\s+des?\s+fonds|total\s+montant\s+fonds)\b", re.I),
     {"op": "sum", "kind": "single", "table": "fonds", "column": "montant", "date_col": "date_lancement"}),
]


@dataclass
class AnalyticsPlan:
    op: str                      
    kind: str                    
    table: Optional[str] = None
    column: Optional[str] = None
    date_col: Optional[str] = None
    targets: Optional[List[Dict[str, Any]]] = None
    year: Optional[int] = None
    


def parse_analytics_plan(message: str) -> Optional[AnalyticsPlan]:
    q = (message or "").strip()
    if not q:
        return None

    year = None
    m = RX_YEAR.search(q)
    if m:
        year = int(m.group(1))

    for rx, spec in SYN_METRICS:
        if rx.search(q):
            return AnalyticsPlan(
                op=spec["op"],
                kind=spec["kind"],
                table=spec.get("table"),
                column=spec.get("column"),
                date_col=spec.get("date_col"),
                targets=spec.get("targets"),
                year=year,
            )

    wants_agg = any(rx.search(q) for rx in [RX_SUM, RX_AVG, RX_COUNT, RX_MIN, RX_MAX])
    if not wants_agg:
        return None

    return None
