# tools/analytics_sql.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set

from sqlalchemy import text
from app.db import engine

from tools.analytics_schema import load_schema, get_display_column, SchemaCache, ForeignKey
from typing import Any, Dict, List
from sqlalchemy import text
from app.db import engine



def run_sql_many(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]

def _ident(name: str) -> str:
    """
    Identifiant SQL safe (table/col). On ne met PAS de user input brut :
    - seulement des noms présents dans information_schema.
    """
    return '"' + name.replace('"', '""') + '"'


@dataclass
class JoinStep:
    src_table: str
    src_col: str
    dst_table: str
    dst_col: str


def find_join_path(src: str, dst: str, sc: SchemaCache, max_depth: int = 3) -> Optional[List[JoinStep]]:
    """
    BFS sur le graphe des FK pour trouver un chemin de jointure src -> dst.
    """
    src = (src or "").lower()
    dst = (dst or "").lower()
    if src == dst:
        return []

    # adjacency using outgoing FKs
    from collections import deque
    q = deque([(src, [])])
    visited: Set[str] = set([src])

    while q:
        table, path = q.popleft()
        if len(path) > max_depth:
            continue

        for fk in sc.fk_out.get(table, []):
            nxt = fk.dst_table
            step = JoinStep(fk.src_table, fk.src_col, fk.dst_table, fk.dst_col)
            if nxt == dst:
                return path + [step]
            if nxt not in visited:
                visited.add(nxt)
                q.append((nxt, path + [step]))

    # try reverse direction (incoming) if needed
    q = deque([(src, [])])
    visited = set([src])

    while q:
        table, path = q.popleft()
        if len(path) > max_depth:
            continue

        for fk in sc.fk_in.get(table, []):
            # fk: other_table -> table, so reverse join
            nxt = fk.src_table
            step = JoinStep(fk.dst_table, fk.dst_col, fk.src_table, fk.src_col)  # reverse direction
            if nxt == dst:
                return path + [step]
            if nxt not in visited:
                visited.add(nxt)
                q.append((nxt, path + [step]))

    return None


def run_sql_scalar(sql: str, params: Dict[str, object]) -> float:
    with engine.begin() as conn:
        val = conn.execute(text(sql), params).scalar()
    return float(val or 0.0)


def run_sql_one(sql: str, params: Dict[str, object]) -> Optional[Dict[str, object]]:
    with engine.begin() as conn:
        row = conn.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def build_fk_lookup_sql(entity_table: str, attr_table: str, entity_name: str) -> Optional[Tuple[str, Dict[str, object]]]:
    """
    Exemple: "actionnaire du projet nommé X"
    - entity_table = projet
    - attr_table = actionnaire
    - entity_name = "Toscani Mannifatture"
    On cherche un FK dans entity_table: <attr_table>_id
    Puis JOIN vers attr_table.id et SELECT attr_table.<display_col>
    """
    sc = load_schema()

    et = (entity_table or "").lower()
    at = (attr_table or "").lower().replace(" ", "_")

    if et not in sc.tables:
        return None

    # 1) trouver la colonne FK candidate
    fk_col = None
    candidates = [
        f"{at}_id",
        f"{at}id",
    ]
    for c in candidates:
        if c in sc.columns.get(et, {}):
            fk_col = c
            break

    # fallback: si attr_table ressemble à "etat" et colonne "etat_id"
    if not fk_col and "etat_id" in sc.columns.get(et, {}) and at.startswith("etat"):
        fk_col = "etat_id"

    if not fk_col:
        return None

    # 2) déterminer table cible via vraie contrainte FK si possible
    target_table = None
    target_pk = "id"
    for fk in sc.fk_out.get(et, []):
        if fk.src_col == fk_col:
            target_table = fk.dst_table
            target_pk = fk.dst_col
            break

    # sinon on suppose attr_table directement
    if not target_table:
        if at in sc.tables:
            target_table = at
        else:
            return None

    disp = get_display_column(target_table, sc) or "id"
    # 3) choisir colonne "nom" de l'entité pour filtrer
    entity_disp = get_display_column(et, sc) or "nom"
    if entity_disp not in sc.columns.get(et, {}):
        # dernier recours: pas de filtre nom => impossible proprement
        return None

    sql = f"""
    SELECT t.{_ident(disp)} AS value
    FROM {_ident(et)} e
    JOIN {_ident(target_table)} t
      ON e.{_ident(fk_col)} = t.{_ident(target_pk)}
    WHERE e.{_ident(entity_disp)} ILIKE :name
    LIMIT 1
    """
    return sql, {"name": f"%{entity_name}%"}
