from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List

from sqlalchemy import text
from app.db import engine
from tools.schema_introspect import SCHEMA, ForeignKeyEdge

def _best_name_col(table: str) -> str:
    cols = SCHEMA.tables.get(table, [])
    for c in ["nom", "denomination", "libelle", "label", "name", "title", "code"]:
        if c in cols:
            return c
    return cols[0] if cols else "id"

def _find_fk_edge(entity_table: str, attr: str) -> Optional[ForeignKeyEdge]:
    edges = SCHEMA.fks_from.get(entity_table, [])
    attr = (attr or "").lower().strip().replace(" ", "_").replace("-", "_")

    for e in edges:
        if e.from_col.lower() == f"{attr}_id":
            return e

    for e in edges:
        if attr in e.from_col.lower():
            return e

    return None

def _entity_row_id(entity_table: str, entity_name: str) -> Optional[int]:
    name_col = _best_name_col(entity_table)
    sql = text(f"SELECT id FROM {entity_table} WHERE {name_col} ILIKE :q ORDER BY id DESC LIMIT 1")
    with engine.begin() as conn:
        r = conn.execute(sql, {"q": f"%{entity_name}%"}).mappings().first()
        return int(r["id"]) if r and r.get("id") is not None else None

def resolve_relation(entity_table: str, attr: str, entity_name: str) -> Optional[Dict[str, Any]]:
    entity_table = (entity_table or "").lower().strip()
    if entity_table not in SCHEMA.tables:
        return None

    eid = _entity_row_id(entity_table, entity_name)
    if eid is None:
        return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "analytics:rel", "hit": False}}

    edge = _find_fk_edge(entity_table, attr)
    if not edge:
        return None

    disp = SCHEMA.find_display_cols(edge.to_table)
    if not disp:
        disp = [_best_name_col(edge.to_table)]

    if disp == ["prenom", "nom"]:
        value_expr = "TRIM(COALESCE(t.prenom,'') || ' ' || COALESCE(t.nom,''))"
    else:
        value_expr = f"t.{disp[0]}"

    sql = text(f"""
        SELECT {value_expr} AS value
        FROM {entity_table} e
        JOIN {edge.to_table} t ON t.{edge.to_col} = e.{edge.from_col}
        WHERE e.id = :id
        LIMIT 1
    """)

    with engine.begin() as conn:
        row = conn.execute(sql, {"id": eid}).mappings().first()
        if not row or row.get("value") is None:
            return {"ok": True, "text": "Je ne sais pas d’après les données disponibles.", "used": {"mode": "analytics:rel", "hit": False}}
        return {
            "ok": True,
            "text": str(row["value"]),
            "used": {"mode": "analytics:rel", "entity_table": entity_table, "attr": attr, "to_table": edge.to_table, "hit": True},
        }
