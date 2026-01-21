# tools/mock_pams.py
from tools.contracts import ToolResult, ok, fail

# Mini dataset (tu peux l’enrichir)
FUNDS = [
    {"id": 252, "denomination": "FCPR MAXULA CAPITAL RETOURNEMENT", "montant": 20040000.0, "frais_gestion": 2.5},
    {"id": 552, "denomination": "FCPR MAXULA EQUITY FUND", "montant": 40080000.0, "frais_gestion": 2.5},
]

PROJECTS = [
    {"id": 1052, "nom": "Toscani Mannifatture", "activite": "Fabrication des chaussures et accessoires", "capital_social": 3368840},
    {"id": 1854, "nom": "TECHNOLATEX", "activite": "Production de gants d'examens médicaux", "capital_social": 2186000},
]


def get_project_by_name(name: str) -> ToolResult:
    name = (name or "").strip().lower()
    if not name:
        return fail("INVALID_INPUT")

    for p in PROJECTS:
        if (p.get("nom") or "").strip().lower() == name:
            return ok(p)

    return fail("PROJECT_NOT_FOUND")


def get_fund_by_name(name: str) -> ToolResult:
    name = (name or "").strip().lower()
    if not name:
        return fail("INVALID_INPUT")

    for f in FUNDS:
        if (f.get("denomination") or "").strip().lower() == name:
            return ok(f)

    return fail("FUND_NOT_FOUND")


def list_funds() -> ToolResult:
    return ok(FUNDS)


# Exemple WRITE tool (mock) -> nécessite confirmation dans MCP
def create_project(payload: dict) -> ToolResult:
    # Ici juste mock : on renvoie un "draft created"
    if not isinstance(payload, dict):
        return fail("INVALID_INPUT")
    return ok({"draft": True, "message": "Projet draft créé (mock).", "payload": payload})
