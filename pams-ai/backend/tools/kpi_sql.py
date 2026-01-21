import os
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from app.db import engine  

# --- sécurité minimale (select) ---
_SELECT_ONLY_RX = re.compile(r"^\s*select\b", re.I)
_FORBIDDEN_RX = re.compile(r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b", re.I)

DEFAULT_TIMEOUT_SEC = float(os.getenv("KPI_SQL_TIMEOUT_SEC", "8"))

class KpiSqlError(Exception):
    pass


def _ensure_safe_sql(sql: str) -> None:
    s = (sql or "").strip()
    if not s:
        raise KpiSqlError("SQL vide")
    if not _SELECT_ONLY_RX.search(s):
        raise KpiSqlError("Seules les requêtes SELECT sont autorisées.")
    if _FORBIDDEN_RX.search(s):
        raise KpiSqlError("SQL interdit (mutation détectée).")


def kpi_query_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Exécute un SELECT agrégé qui renvoie 1 valeur (ex: SUM).
    Retourne float ou None.
    """
    _ensure_safe_sql(sql)
    params = params or {}
    with engine.begin() as conn:
        try:
            conn.execute(text(f"SET LOCAL statement_timeout = {int(DEFAULT_TIMEOUT_SEC * 1000)}"))
        except Exception:
            pass

        row = conn.execute(text(sql), params).first()

    if not row:
        return None

    val = row[0]
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return float(str(val))


def kpi_query_rows(sql: str, params: Optional[Dict[str, Any]] = None, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Exécute un SELECT qui renvoie plusieurs lignes (ex: GROUP BY).
    Retourne une liste de dict.
    """
    _ensure_safe_sql(sql)
    params = params or {}

    if "limit" not in (sql or "").lower():
        sql = sql.rstrip().rstrip(";") + f" LIMIT {int(limit)}"

    with engine.begin() as conn:
        try:
            conn.execute(text(f"SET LOCAL statement_timeout = {int(DEFAULT_TIMEOUT_SEC * 1000)}"))
        except Exception:
            pass
        rows = conn.execute(text(sql), params).mappings().all()

    return [dict(r) for r in rows]
