# core/policy.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Policy:
    role: str = "analyst"  

    # limites retrieval / contexte
    max_top_k: int = 12
    max_context_chars: int = 12000
    #
    allowed_source_types: Optional[List[str]] = None

    # outils autorisés
    can_use_crud: bool = False
    can_use_reporting: bool = False


def get_policy(role: str) -> Policy:
    r = (role or "analyst").lower().strip()

    if r == "viewer":
        return Policy(
            role="viewer",
            max_top_k=8,
            max_context_chars=9000,
            allowed_source_types=None,   
            can_use_reporting=False,
        )

    if r == "admin":
        return Policy(
            role="admin",
            max_top_k=12,
            max_context_chars=14000,
            allowed_source_types=None,
            can_use_crud=True,
            can_use_reporting=True,
        )

    # analyst par défaut
    return Policy(
        role="analyst",
        max_top_k=10,
        max_context_chars=12000,
        allowed_source_types=None,
        can_use_crud=False,       
        can_use_reporting=True,
    )


def clamp_top_k(top_k: int, policy: Policy) -> int:
    k = int(top_k or 8)
    if k < 1:
        k = 1
    if k > policy.max_top_k:
        k = policy.max_top_k
    return k


def filter_source_types(requested: Optional[List[str]], policy: Policy) -> Optional[List[str]]:
    """
    - requested: filtre venant du router/agent (ex: ["maxula:fonds"])
    - policy.allowed_source_types: filtre global autorisé
    Retourne la liste finale autorisée (ou None => pas de filtre)
    """
    if not requested and not policy.allowed_source_types:
        return None

    req = [s for s in (requested or []) if s]
    allowed = [s for s in (policy.allowed_source_types or []) if s]

    if not allowed:
        return req or None

    if not req:
        return allowed

    # intersection
    req_set = set(req)
    out = [s for s in allowed if s in req_set]
    return out or []


def enforce_tool(tool_name: str, policy: Policy) -> None:
    """
    Lève une exception si tool non autorisé pour le rôle.
    """
    t = tool_name.strip().lower()

    if t in ("pams_api", "pams_api_get", "search_chunks"):
        return

    if t in ("n8n_report", "n8n") and not policy.can_use_reporting:
        raise PermissionError(f"Rôle '{policy.role}' : reporting non autorisé")

    if t in ("crud", "pams_api_write") and not policy.can_use_crud:
        raise PermissionError(f"Rôle '{policy.role}' : CRUD non autorisé")
