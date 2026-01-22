from typing import Any, Dict, List, Optional
import re

from rag.retrieve_core import hybrid_retrieve
from rag.answer import structured_answer_from_chunks
from rag.langchain_answer import answer as lc_rag_answer
from agent.lc_agent import run_agent

from tools.analytics_parser import parse_intent
from tools.analytics_service import run_analytics
from tools.sql_ai_engine import try_answer_sql


def _sources_from_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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


def _detect_report_intent(message: str) -> bool:
    return bool(re.search(r"\b(rapport|reporting|export|excel|pdf|hebdo|mensuel)\b", message or "", re.I))


def _detect_crud_intent(message: str) -> bool:
    return bool(re.search(r"\b(ajoute|ajouter|crée|creer|modifier|supprimer|mets à jour|mettre à jour)\b", message or "", re.I))


def _early_rel_analytics(message: str) -> Optional[Dict[str, Any]]:
    intent = parse_intent(message or "")
    if not intent or intent.kind != "rel":
        return None
    if not intent.entity_table or not intent.attr_table:
        return None
    if not intent.entity_name:
        return None
    a = run_analytics(message, debug=False)
    if a and a.get("ok"):
        return a
    return None


def chat_pipeline(
    message: str,
    top_k: int = 8,
    model: str = "llama3.2",
    role: str = "viewer",
    debug: bool = False,
    mode: str = "rag",
) -> Dict[str, Any]:

    if _detect_report_intent(message):
        return {
            "answer": "Je peux générer un reporting. Dis-moi le fonds/période et le format (PDF/Excel).",
            "sources": [],
            "suggested_actions": [{"type": "report", "payload": {"name": "ask_user_params"}}],
            "navigation": [],
            "used": {"mode": "router:report_hint", "debug": debug},
        }

    if _detect_crud_intent(message) and role != "viewer":
        return {
            "answer": "Je peux t’aider à faire cette action. Donne les champs et tu confirmeras avant enregistrement.",
            "sources": [],
            "suggested_actions": [{"type": "crud", "payload": {"name": "draft_action_from_user"}}],
            "navigation": [],
            "used": {"mode": "router:crud_hint", "debug": debug},
        }

    if mode == "agent":
        res = run_agent(message, model=model, role=role)
        text = res.get("text") or "Je ne sais pas d’après les données disponibles."
        return {
            "answer": text,
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {"mode": "agent-tools", "model": model, "role": role, "debug": debug},
        }

    early = _early_rel_analytics(message)
    if early:
        return {
            "answer": early["text"],
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": early.get("used") or {"mode": "analytics:rel", "debug": debug},
        }

    a = run_analytics(message, debug=debug)
    if a and a.get("ok"):
        return {
            "answer": a["text"],
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": a.get("used") or {"mode": "analytics", "debug": debug},
        }

    s = try_answer_sql(message, debug=debug)
    if s and s.get("ok"):
        return {
            "answer": s["text"],
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": s.get("used") or {"mode": "sql", "debug": debug},
        }

    chunks, scope, domain = hybrid_retrieve(message, top_k=top_k)
    sources = _sources_from_chunks(chunks)
    navigation = _suggest_navigation(chunks)

    structured = structured_answer_from_chunks(message, chunks)
    if structured:
        return {
            "answer": structured,
            "sources": sources,
            "suggested_actions": [],
            "navigation": navigation,
            "used": {"mode": "structured-first", "domain": domain, "scope": scope, "top_k": top_k, "debug": debug},
        }

    llm_text = lc_rag_answer(message, top_k=top_k, model=model)
    return {
        "answer": llm_text,
        "sources": sources,
        "suggested_actions": [],
        "navigation": navigation,
        "used": {"mode": "llm-fallback", "domain": domain, "scope": scope, "top_k": top_k, "model": model, "debug": debug},
    }
