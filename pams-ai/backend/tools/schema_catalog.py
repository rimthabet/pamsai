import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

MAXULA_DB_URL = os.getenv(
    "MAXULA_DB_URL",
    "postgresql+psycopg2://postgres:rimthabet@localhost:5432/maxula"
)

LABEL_CANDIDATES = [
    "nom", "libelle", "label", "denomination", "raison_sociale",
    "code", "reference", "ref", "titre", "name"
]

@dataclass(frozen=True)
class FKEdge:
    """Edge: src_table.src_col -> ref_table.ref_col"""
    src_table: str
    src_col: str
    ref_table: str
    ref_col: str

@dataclass
class TableInfo:
    name: str
    pk_cols: List[str]
    columns: Dict[str, str]          
    numeric_cols: List[str]
    text_cols: List[str]
    date_cols: List[str]
    fks: List[FKEdge]
    label_col: Optional[str]         
class SchemaCatalog:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.tables: Dict[str, TableInfo] = {}
        self._build()

    def _is_numeric(self, type_name: str) -> bool:
        t = (type_name or "").lower()
        return any(x in t for x in ["int", "numeric", "decimal", "float", "double", "real"])

    def _is_text(self, type_name: str) -> bool:
        t = (type_name or "").lower()
        return any(x in t for x in ["char", "text", "varchar", "string", "uuid"])

    def _is_date(self, type_name: str) -> bool:
        t = (type_name or "").lower()
        return any(x in t for x in ["date", "time"])

    def _pick_label_col(self, cols: List[str]) -> Optional[str]:
        cols_l = [c.lower() for c in cols]
        for cand in LABEL_CANDIDATES:
            if cand in cols_l:
                return cols[cols_l.index(cand)]
        return None

    def _build(self):
        insp = inspect(self.engine)
        for tname in insp.get_table_names():
            cols = insp.get_columns(tname)
            col_types: Dict[str, str] = {c["name"]: str(c["type"]) for c in cols}
            pk = insp.get_pk_constraint(tname).get("constrained_columns") or []

            numeric_cols = [c for c, tn in col_types.items() if self._is_numeric(tn)]
            text_cols = [c for c, tn in col_types.items() if self._is_text(tn)]
            date_cols = [c for c, tn in col_types.items() if self._is_date(tn)]
            label_col = self._pick_label_col(list(col_types.keys()))

            fks: List[FKEdge] = []
            for fk in insp.get_foreign_keys(tname) or []:
                ref_table = fk.get("referred_table")
                ccols = fk.get("constrained_columns") or []
                rcols = fk.get("referred_columns") or []
                if not ref_table or not ccols or not rcols:
                    continue
                for sc, rc in zip(ccols, rcols):
                    fks.append(FKEdge(src_table=tname, src_col=sc, ref_table=ref_table, ref_col=rc))

            self.tables[tname] = TableInfo(
                name=tname,
                pk_cols=list(pk),
                columns=col_types,
                numeric_cols=numeric_cols,
                text_cols=text_cols,
                date_cols=date_cols,
                fks=fks,
                label_col=label_col,
            )

    @staticmethod
    def default() -> "SchemaCatalog":
        engine = create_engine(MAXULA_DB_URL, pool_pre_ping=True)
        return SchemaCatalog(engine)
