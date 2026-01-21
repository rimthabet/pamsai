# tools/kpi_service.py
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text

from app.db import engine
from tools.kpi_router import route_kpi
from tools.kpi_catalog import get_kpi_def


# ---------
# Parsing
# ---------
YEAR_RX = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")

def extract_year(message: str) -> Optional[int]:
    """Extrait une année (ex: 2024) si présente dans le texte."""
    q = message or ""
    m = YEAR_RX.search(q)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


# ---------
# SQL exec
# ---------
def _scalar(sql: str, params: Optional[Dict[str, Any]] = None) -> float:
    """Exécute un SQL qui retourne une seule valeur."""
    params = params or {}
    with engine.begin() as conn:
        val = conn.execute(text(sql), params).scalar()
    try:
        return float(val or 0.0)
    except Exception:
        return 0.0


# ---------
# Rendering
# ---------
def render_kpi_answer(value: float, unit: str = "TND", year: Optional[int] = None) -> str:
    # format 1-ligne (tu peux changer)
    s = f"{value:,.0f}".replace(",", " ")
    if year is not None:
        return f"Le total est {s} {unit} (année {year})."
    return f"Le total est {s} {unit}."


# ---------
# Main KPI runner
# ---------
def run_kpi(message: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Retour:
      None si pas KPI
      sinon dict:
        { "kpi": str, "value": float, "unit": str, "year": Optional[int], "sql": str, "params": dict }
    """
    q = (message or "").strip()
    if not q:
        return None

    kpi_name = route_kpi(q)
    if not kpi_name:
        return None

    kdef = get_kpi_def(kpi_name)
    if not kdef:
        return None

    year = extract_year(q)
    unit = kdef.get("unit") or "TND"

    # ✅ IMPORTANT:
    # - si année demandée: on utilise sql_year (ou on dérive du sql_all si tu veux)
    # - sinon: on utilise sql_all (ou sql_one)
    sql_all = (kdef.get("sql_all") or kdef.get("sql_one") or "").strip()
    sql_year = (kdef.get("sql_year") or "").strip()

    params: Dict[str, Any] = {}

    if year is not None:
        # si sql_year n'existe pas, on essaye de fallback propre
        if not sql_year:
            # fallback simple : si ton sql_all contient déjà un filtre :year -> erreur
            # donc ici on refuse plutôt que d'exécuter faux
            # (tu peux aussi décider de calculer sql_year automatiquement plus tard)
            if debug:
                return {
                    "kpi": kpi_name,
                    "value": 0.0,
                    "unit": unit,
                    "year": year,
                    "sql": "",
                    "params": {"year": year},
                    "debug": {"error": "sql_year missing for this KPI"},
                }
            return {
                "kpi": kpi_name,
                "value": 0.0,
                "unit": unit,
                "year": year,
                "sql": "",
                "params": {"year": year},
            }

        params["year"] = year
        value = _scalar(sql_year, params)

        out = {"kpi": kpi_name, "value": value, "unit": unit, "year": year, "sql": sql_year, "params": params}
        if debug:
            out["debug"] = {"matched": kpi_name, "year": year}
        return out

    # pas d'année -> sql_all
    if not sql_all:
        return None

    value = _scalar(sql_all, params)
    out = {"kpi": kpi_name, "value": value, "unit": unit, "year": None, "sql": sql_all, "params": params}
    if debug:
        out["debug"] = {"matched": kpi_name, "year": None}
    return out
