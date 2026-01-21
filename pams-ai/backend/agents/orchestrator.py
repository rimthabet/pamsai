from __future__ import annotations
from typing import Any, Dict, List, Tuple

from core.policy import get_policy
from agents.router import build_plan, AgentPlan

from rag.retrieve_core import hybrid_retrieve  
from rag.answer import structured_answer_from_chunks
from rag.langchain_answer import answer as lc_answer

from tools import n8n_client


def _suggest_navigation(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    nav = []
    for ch in chunks:
        md = ch.get("metadata") or {}
        table = md.get("table")
        pk = md.get("pk") or {}
        if table == "projet" and pk.get("id"):
            nav.append({"type": "open_page", "payload": {"route": f"/projects/{pk['id']}"}})
            break
        if table == "fonds" and pk.get("id"):
            nav.append({"type": "open_page", "payload": {"route": f"/fonds/{pk['id']}"}})
            break
    return nav


def _format_sources(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for ch in chunks:
        out.append({
            "id": int(ch.get("id")),
            "source_type": ch.get("source_type") or "",
            "source_id": str(ch.get("source_id") or ""),
            "score": float(ch.get("score") or 0.0),
            "metadata": ch.get("metadata") or {},
        })
    return out


def run_agent(
    message: str,
    role: str = "analyst",
    top_k: int = 8,
    model: str = "llama3.2",
    debug: bool = False
) -> Dict[str, Any]:
    policy = get_policy(role)
    plan: AgentPlan = build_plan(message)

    top_k = min(int(top_k or 8), policy.max_top_k)

    used: Dict[str, Any] = {
        "role": role,
        "intent": plan.intent,
        "domain": plan.domain,
        "source_types": plan.source_types,
        "entity_hint": plan.entity_hint,
        "top_k": top_k,
        "model": model,
        "policy": {"allowed_tools": policy.allowed_tools},
    }

    # Report intent → propose action n8n 
    if plan.intent == "report":
        if "n8n_report" not in policy.allowed_tools:
            return {
                "answer": "Vous n’avez pas les droits pour générer un reporting.",
                "sources": [],
                "suggested_actions": [],
                "navigation": [],
                "used": {**used, "mode": "denied"},
            }
        return {
            "answer": "Je peux générer un reporting via n8n. Indique le fonds et la période, puis je te proposerai l’action à confirmer.",
            "sources": [],
            "suggested_actions": [{"type": "report", "payload": {"name": "generate_report", "needs": ["fonds", "periode"]}}],
            "navigation": [],
            "used": {**used, "mode": "router:report"},
        }

    #  CRUD intent → proposer action 
    if plan.intent == "crud":
        if "pams_write" not in policy.allowed_tools:
            return {
                "answer": "Vous n’avez pas les droits pour modifier les données. Je peux seulement consulter et expliquer.",
                "sources": [],
                "suggested_actions": [],
                "navigation": [],
                "used": {**used, "mode": "denied"},
            }
        return {
            "answer": "Je peux préparer l’action (pré-remplissage + validation) puis tu confirmeras avant enregistrement.",
            "sources": [],
            "suggested_actions": [{"type": "crud", "payload": {"name": "draft_action_from_user"}}],
            "navigation": [],
            "used": {**used, "mode": "router:crud"},
        }

    # Guide intent → réponse “procédurale” (souvent sans RAG), mais on peut utiliser retrieval
    # QA / Guide → retrieval
    if "retrieve" not in policy.allowed_tools:
        return {
            "answer": "Récupération documentaire non autorisée pour ce rôle.",
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {**used, "mode": "denied"},
        }

    chunks, used_scope, used_domain = hybrid_retrieve(
        message,
        top_k=top_k,
        source_types=plan.source_types,  
        entity_hint=plan.entity_hint,
    )

    sources = _format_sources(chunks)
    navigation = _suggest_navigation(chunks)

    
    structured = structured_answer_from_chunks(message, chunks)
    if structured:
        return {
            "answer": structured,
            "sources": sources,
            "suggested_actions": [],
            "navigation": navigation,
            "used": {**used, "mode": "structured-first", "scope_used": used_scope, "domain_used": used_domain},
        }

    # LLM fallback (LangChain + llama)
    llm_text = lc_answer(message, top_k=top_k, model=model)
    return {
        "answer": llm_text,
        "sources": sources,
        "suggested_actions": [],
        "navigation": navigation,
        "used": {**used, "mode": "llm-fallback", "scope_used": used_scope, "domain_used": used_domain},
    }
