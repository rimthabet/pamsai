# tools/schema_graph.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text

from app.db import engine

DEFAULT_SCHEMA = "public"


@dataclass(frozen=True)
class FKEdge:
    src_table: str
    src_col: str
    dst_table: str
    dst_col: str


@dataclass
class SchemaGraph:
    schema: str
    tables: List[str]
    columns: Dict[str, List[Tuple[str, str]]]      # table -> [(col, data_type)]
    fks: List[FKEdge]
    out_edges: Dict[str, List[FKEdge]]             # src_table -> edges
    in_edges: Dict[str, List[FKEdge]]              # dst_table -> edges


def _norm(name: str) -> str:
    return (name or "").strip().lower()


def load_schema_graph(schema: str = DEFAULT_SCHEMA,
                      exclude_tables: Optional[List[str]] = None) -> SchemaGraph:
    exclude = set(_norm(t) for t in (exclude_tables or []))

    # tables
    sql_tables = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_type='BASE TABLE'
        ORDER BY table_name
    """)

    # columns
    sql_cols = text("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema
        ORDER BY table_name, ordinal_position
    """)

    # foreign keys
    sql_fks = text("""
        SELECT
          tc.table_name AS src_table,
          kcu.column_name AS src_col,
          ccu.table_name AS dst_table,
          ccu.column_name AS dst_col
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = :schema
        ORDER BY src_table, src_col
    """)

    with engine.begin() as conn:
        tables = [r[0] for r in conn.execute(sql_tables, {"schema": schema}).all()]
        tables = [t for t in tables if _norm(t) not in exclude]

        cols_rows = conn.execute(sql_cols, {"schema": schema}).all()
        columns: Dict[str, List[Tuple[str, str]]] = {}
        for t, c, dt in cols_rows:
            if _norm(t) in exclude:
                continue
            columns.setdefault(t, []).append((c, dt))

        fk_rows = conn.execute(sql_fks, {"schema": schema}).all()
        fks: List[FKEdge] = []
        for st, sc, dt, dc in fk_rows:
            if _norm(st) in exclude or _norm(dt) in exclude:
                continue
            fks.append(FKEdge(st, sc, dt, dc))

    out_edges: Dict[str, List[FKEdge]] = {}
    in_edges: Dict[str, List[FKEdge]] = {}
    for e in fks:
        out_edges.setdefault(e.src_table, []).append(e)
        in_edges.setdefault(e.dst_table, []).append(e)

    return SchemaGraph(
        schema=schema,
        tables=tables,
        columns=columns,
        fks=fks,
        out_edges=out_edges,
        in_edges=in_edges,
    )
