# tools/schema_cache.py
from __future__ import annotations
import os
from typing import Optional, List
from tools.schema_graph import SchemaGraph, load_schema_graph

_cached: Optional[SchemaGraph] = None

def get_schema_graph(force_reload: bool = False) -> SchemaGraph:
    global _cached
    if _cached is not None and not force_reload:
        return _cached

    excl = os.getenv("MAXULA_EXCLUDE_TABLES", "")
    exclude_tables: List[str] = [x.strip() for x in excl.split(",") if x.strip()]
    schema = os.getenv("MAXULA_SCHEMA", "public")

    _cached = load_schema_graph(schema=schema, exclude_tables=exclude_tables)
    return _cached
