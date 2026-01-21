# tools/analytics_schema.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

from sqlalchemy import text
from app.db import engine


@dataclass
class ColumnInfo:
    name: str
    data_type: str 
    is_nullable: bool


@dataclass
class ForeignKey:
    src_table: str
    src_col: str
    dst_table: str
    dst_col: str


@dataclass
class SchemaCache:
    tables: Set[str]
    columns: Dict[str, Dict[str, ColumnInfo]]         
    fks: List[ForeignKey]
    fk_out: Dict[str, List[ForeignKey]]                
    fk_in: Dict[str, List[ForeignKey]]                 


_CACHE: Optional[Tuple[float, SchemaCache]] = None
_TTL_SECONDS = float(os.getenv("ANALYTICS_SCHEMA_TTL", "300"))


def _normalize_table(t: str) -> str:
    return (t or "").strip().lower()


def load_schema(force: bool = False) -> SchemaCache:
    global _CACHE
    now = time.time()
    if not force and _CACHE:
        ts, sc = _CACHE
        if now - ts < _TTL_SECONDS:
            return sc

    sql_cols = text("""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """)

    
    sql_fk = text("""
        SELECT
          src.relname AS src_table,
          sa.attname  AS src_col,
          dst.relname AS dst_table,
          da.attname  AS dst_col
        FROM pg_constraint c
        JOIN pg_class src ON src.oid = c.conrelid
        JOIN pg_class dst ON dst.oid = c.confrelid
        JOIN unnest(c.conkey)  WITH ORDINALITY AS src_cols(attnum, ord) ON TRUE
        JOIN unnest(c.confkey) WITH ORDINALITY AS dst_cols(attnum, ord) ON src_cols.ord = dst_cols.ord
        JOIN pg_attribute sa ON sa.attrelid = src.oid AND sa.attnum = src_cols.attnum
        JOIN pg_attribute da ON da.attrelid = dst.oid AND da.attnum = dst_cols.attnum
        WHERE c.contype = 'f'
    """)

    columns: Dict[str, Dict[str, ColumnInfo]] = {}
    tables: Set[str] = set()

    with engine.begin() as conn:
        for r in conn.execute(sql_cols).mappings().all():
            t = _normalize_table(r["table_name"])
            c = (r["column_name"] or "").strip().lower()
            tables.add(t)
            columns.setdefault(t, {})[c] = ColumnInfo(
                name=c,
                data_type=(r["data_type"] or "").strip().lower(),
                is_nullable=(str(r["is_nullable"]).upper() == "YES")
            )

        fks: List[ForeignKey] = []
        for r in conn.execute(sql_fk).mappings().all():
            fks.append(ForeignKey(
                src_table=_normalize_table(r["src_table"]),
                src_col=(r["src_col"] or "").strip().lower(),
                dst_table=_normalize_table(r["dst_table"]),
                dst_col=(r["dst_col"] or "").strip().lower(),
            ))

    fk_out: Dict[str, List[ForeignKey]] = {}
    fk_in: Dict[str, List[ForeignKey]] = {}
    for fk in fks:
        fk_out.setdefault(fk.src_table, []).append(fk)
        fk_in.setdefault(fk.dst_table, []).append(fk)

    sc = SchemaCache(
        tables=tables,
        columns=columns,
        fks=fks,
        fk_out=fk_out,
        fk_in=fk_in,
    )
    _CACHE = (now, sc)
    return sc


def table_has_column(table: str, col: str, sc: Optional[SchemaCache] = None) -> bool:
    sc = sc or load_schema()
    t = _normalize_table(table)
    c = (col or "").strip().lower()
    return t in sc.columns and c in sc.columns[t]


def get_display_column(table: str, sc: Optional[SchemaCache] = None) -> Optional[str]:
    """
    Heuristique : choisir la meilleure colonne 'nom' pour afficher un libellé.
    """
    sc = sc or load_schema()
    t = _normalize_table(table)
    cols = sc.columns.get(t, {})
    for candidate in ["nom", "denomination", "libelle", "raison_sociale", "name", "title", "alias"]:
        if candidate in cols:
            return candidate
    return None
