# mcp/guard.py
from mcp.policy import TOOL_POLICY

# Rôles : viewer < analyst < admin
ROLE_ALLOWED = {
    "viewer": {"read"},
    "analyst": {"read", "report"},
    "admin": {"read", "report", "write"},
}

def allow_tool(role: str, tool_name: str) -> bool:
    role = (role or "viewer").lower()
    mode = TOOL_POLICY.get(tool_name, "read")
    allowed = ROLE_ALLOWED.get(role, {"read"})
    return mode in allowed

def needs_confirmation(tool_name: str) -> bool:
    # Tout ce qui est write = confirmation obligatoire
    return TOOL_POLICY.get(tool_name) == "write"
