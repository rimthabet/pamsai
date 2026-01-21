from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from tools.schema_cache import SchemaCache

RX_GOOD_NUM_COL = re.compile(r"(montant|capital|cout|co[uû]t|frais|valeur|prix|ratio)", re.I)
RX_GOOD_DATE_COL = re.compile(r"(date|created|updated)", re.I)

@dataclass
class Target:
    table: str
    col: str
    date_col: Optional[str]  
    score: float


def _boost_table(table: str, hint: str) -> float:
    t = (table or "").lower()
    h = (hint or "").lower()
    boost = 0.0
    if "liberation" in t or "libération" in t:
        boost += 0.6 if ("investi" in h or "liber" in h) else 0.2
    if "souscription" in t:
        boost += 0.6 if ("actif" in h or "souscrip" in h or "montant" in h) else 0.2
    if "fonds" in t:
        boost += 0.2
    if "projet" in t:
        boost += 0.2
    return boost


def _pick_date_col(schema: SchemaCache, table: str) -> Optional[str]:
    ti = schema.get_table(table)
    if not ti:
        return None
    # priorité : date_* puis created_on puis updated_on
    candidates = []
    for c in ti.columns.values():
        if not c.is_date:
            continue
        s = 0.0
        if c.name.lower().startswith("date_"):
            s += 2.0
        if "created" in c.name.lower():
            s += 1.2
        if "updated" in c.name.lower():
            s += 1.0
        if RX_GOOD_DATE_COL.search(c.name):
            s += 0.3
        candidates.append((s, c.name))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def choose_target(schema: SchemaCache, query: str, measure_hint: str) -> Optional[Target]:
    q = (query or "").lower()
    mh = (measure_hint or "").lower()

    scored: List[Target] = []

    for tname, ti in schema.tables.items():
        # ignore tables techniques 
        for c in ti.columns.values():
            if not c.is_numeric:
                continue

            s = 0.0
            # nom de colonne
            if RX_GOOD_NUM_COL.search(c.name):
                s += 1.2

            # match hint
            if mh and mh in c.name.lower():
                s += 1.6
            if mh and mh in tname.lower():
                s += 0.8

            # mots de la question
            if "invest" in q and ("liber" in tname.lower() or "liber" in c.name.lower()):
                s += 1.5
            if "actif" in q and "souscription" in tname.lower():
                s += 1.5

            # boost table
            s += _boost_table(tname, mh)

            if s > 0.0:
                date_col = _pick_date_col(schema, tname)
                scored.append(Target(table=tname, col=c.name, date_col=date_col, score=s))

    if not scored:
        return None

    scored.sort(key=lambda x: x.score, reverse=True)
    return scored[0]
