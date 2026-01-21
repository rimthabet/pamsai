# mcp/policy.py
from typing import Dict, Literal

ToolMode = Literal["read", "write", "report"]

TOOL_POLICY: Dict[str, ToolMode] = {
    "tool_get_project_by_name": "read",
    "tool_get_fund_by_name": "read",
    "tool_list_funds": "read",
    "tool_create_project": "write",
}
