# tools/langchain_tools.py
from langchain_core.tools import tool
from mcp.guard import allow_tool, needs_confirmation

from tools.mock_pams import (
    get_project_by_name,
    get_fund_by_name,
    list_funds,
    create_project,
)

# On passe role via un "context" simple.
# LangChain tools n’ont pas nativement (dans tous cas) un context global,
# donc on utilise une variable module (simple & efficace pour démarrer).
_CURRENT_ROLE = "viewer"

def set_current_role(role: str):
    global _CURRENT_ROLE
    _CURRENT_ROLE = role or "viewer"


def _guard(tool_name: str):
    if not allow_tool(_CURRENT_ROLE, tool_name):
        return {"ok": False, "data": None, "error": f"FORBIDDEN: role={_CURRENT_ROLE} tool={tool_name}"}
    if needs_confirmation(tool_name):
        # On ne fait PAS l’action directement. On renvoie une proposition.
        return {"ok": False, "data": None, "error": f"CONFIRMATION_REQUIRED: tool={tool_name}"}
    return None


@tool
def tool_get_project_by_name(name: str) -> dict:
    """(READ) Récupère un projet par son nom. Retourne JSON {ok,data,error}."""
    g = _guard("tool_get_project_by_name")
    if g: return g
    return get_project_by_name(name)


@tool
def tool_get_fund_by_name(name: str) -> dict:
    """(READ) Récupère un fonds par sa dénomination exacte."""
    g = _guard("tool_get_fund_by_name")
    if g: return g
    return get_fund_by_name(name)


@tool
def tool_list_funds() -> dict:
    """(READ) Liste des fonds (mock)."""
    g = _guard("tool_list_funds")
    if g: return g
    return list_funds()


@tool
def tool_create_project(payload: dict) -> dict:
    """
    (WRITE) Crée un projet. Nécessite confirmation.
    Retourne JSON. (mock)
    """

    g = _guard("tool_create_project")
    if g: return g
    return create_project(payload)
