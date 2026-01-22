from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlalchemy import inspect
from app.db import engine

EXCLUDE_TABLES = {t.strip() for t in os.getenv("MAXULA_EXCLUDE_TABLES", "").split(",") if t.strip()}

DISPLAY_COL_CANDIDATES = [
    "nom", "denomination", "libelle", "label", "raison_sociale", "name", "title", "code",
    "prenom", "first_name", "last_name"
]

@dataclass(frozen=True)
class ForeignKeyEdge:
    from_table: str
    from_col: str
    to_table: str
    to_col: str

class SchemaCache:
    def __init__(self) -> None:
        self.tables: Dict[str, List[str]] = {}
        self.fks_from: Dict[str, List[ForeignKeyEdge]] = {}
        self.pk_cols: Dict[str, List[str]] = {}
        self._loaded = False

    def load(self) -> "SchemaCache":
        if self._loaded:
            return self
        insp = inspect(engine)
        for t in insp.get_table_names():
            if t in EXCLUDE_TABLES:
                continue
            cols = [c["name"] for c in insp.get_columns(t)]
            self.tables[t] = cols
            pk = insp.get_pk_constraint(t).get("constrained_columns") or []
            self.pk_cols[t] = pk
            edges: List[ForeignKeyEdge] = []
            for fk in insp.get_foreign_keys(t):
                if not fk.get("referred_table"):
                    continue
                rt = fk["referred_table"]
                if rt in EXCLUDE_TABLES:
                    continue
                lcols = fk.get("constrained_columns") or []
                rcols = fk.get("referred_columns") or []
                for lc, rc in zip(lcols, rcols):
                    edges.append(ForeignKeyEdge(from_table=t, from_col=lc, to_table=rt, to_col=rc))
            self.fks_from[t] = edges
        self._loaded = True
        return self

    def find_display_cols(self, table: str) -> List[str]:
        cols = self.tables.get(table, [])
        found = [c for c in DISPLAY_COL_CANDIDATES if c in cols]
        if "prenom" in cols and "nom" in cols:
            return ["prenom", "nom"]
        if found:
            return found[:2]
        for c in cols:
            if c.endswith("_name") or c.endswith("_label"):
                return [c]
        return []

SCHEMA = SchemaCache().load()
