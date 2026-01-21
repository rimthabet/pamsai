# tools/kpi_catalog.py
from typing import Dict, Any, Optional

KPI_CATALOG: Dict[str, Dict[str, Any]] = {
    "total_actif": {
        "label": "Total actif",
        "unit": "TND",
        "sql_all": "SELECT COALESCE(SUM(montant_souscription), 0) FROM souscription",
        "sql_year": """
            SELECT COALESCE(SUM(montant_souscription), 0)
            FROM souscription
            WHERE EXTRACT(YEAR FROM date_souscription) = :year
        """,
    },

    "investi_actions": {
        "label": "Investi en actions",
        "unit": "TND",
        "sql_all": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_action",
        "sql_year": """
            SELECT COALESCE(SUM(montant_liberation), 0)
            FROM inv_liberation_action
            WHERE EXTRACT(YEAR FROM date_liberation) = :year
        """,
    },
    "investi_oca": {
        "label": "Investi en OCA",
        "unit": "TND",
        "sql_all": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_oca",
        "sql_year": """
            SELECT COALESCE(SUM(montant_liberation), 0)
            FROM inv_liberation_oca
            WHERE EXTRACT(YEAR FROM date_liberation) = :year
        """,
    },
    "investi_cca": {
        "label": "Investi en CCA",
        "unit": "TND",
        "sql_all": "SELECT COALESCE(SUM(montant_liberation), 0) FROM inv_liberation_cca",
        "sql_year": """
            SELECT COALESCE(SUM(montant_liberation), 0)
            FROM inv_liberation_cca
            WHERE EXTRACT(YEAR FROM date_liberation) = :year
        """,
    },

    "total_investi": {
        "label": "Total investi",
        "unit": "TND",
        "sql_all": """
            SELECT
              COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_action), 0)
            + COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_oca), 0)
            + COALESCE((SELECT SUM(montant_liberation) FROM inv_liberation_cca), 0)
        """,
        "sql_year": """
            SELECT
              COALESCE((
                SELECT SUM(montant_liberation)
                FROM inv_liberation_action
                WHERE EXTRACT(YEAR FROM date_liberation) = :year
              ), 0)
            + COALESCE((
                SELECT SUM(montant_liberation)
                FROM inv_liberation_oca
                WHERE EXTRACT(YEAR FROM date_liberation) = :year
              ), 0)
            + COALESCE((
                SELECT SUM(montant_liberation)
                FROM inv_liberation_cca
                WHERE EXTRACT(YEAR FROM date_liberation) = :year
              ), 0)
        """,
    },
}

def get_kpi_def(name: str) -> Optional[Dict[str, Any]]:
    return KPI_CATALOG.get(name)
