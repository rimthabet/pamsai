from __future__ import annotations
import os
import requests
from typing import Any, Dict


N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()


class N8nError(RuntimeError):
    pass


def run_report(payload: Dict[str, Any], timeout: int = 60) -> Any:
    if not N8N_WEBHOOK_URL:
        raise N8nError("N8N_WEBHOOK_URL n'est pas défini")
    r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=timeout)
    if r.status_code >= 400:
        raise N8nError(f"n8n -> {r.status_code}: {r.text[:300]}")
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}
