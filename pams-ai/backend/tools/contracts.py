from typing import Any, Dict, Optional, TypedDict


class ToolResult(TypedDict):
    ok: bool
    data: Any
    error: Optional[str]


def ok(data: Any) -> ToolResult:
    return {"ok": True, "data": data, "error": None}


def fail(error: str, data: Any = None) -> ToolResult:
    return {"ok": False, "data": data, "error": error}
