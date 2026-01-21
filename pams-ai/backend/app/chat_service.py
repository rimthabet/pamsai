# app/chat_service.py
from typing import Any, Dict, List
import re

from rag.retrieve_core import hybrid_retrieve
from rag.answer import structured_answer_from_chunks
from rag.langchain_answer import answer as lc_rag_answer

from tools.kpi_router import route_kpi
from tools.kpi_service import run_kpi, render_kpi_answer

from tools.relational_qa import relational_answer_one_line

# optionnel (si tu as un agent tool-calling)
try:
    from agent.lc_agent import run_agent
except Exception:
    run_agent = None


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


def _detect_definition_intent(message: str) -> bool:
    # ex: "c'est quoi un fonds", "définition fonds"
    return bool(re.search(r"\b(c['’]est quoi|ça veut dire|definition|définition)\b", message or "", re.I))


def chat_pipeline(
    message: str,
    top_k: int = 8,
    model: str = "llama3.2",
    role: str = "viewer",
    debug: bool = False,
    mode: str = "rag",
) -> Dict[str, Any]:
    """
    Router:
      1) reporting
      2) crud
      3) KPI (SQL déterministe)
      4) Relational QA (SQL + joins FK automatiques)
      5) mode agent (si activé)
      6) RAG (structured-first -> LLM fallback)
    """

    msg = message or ""

    # 1) reporting
    if _detect_report_intent(msg):
        return {
            "answer": "Je peux générer un reporting. Dis-moi le fonds/période et le format (PDF/Excel).",
            "sources": [],
            "suggested_actions": [{"type": "report", "payload": {"name": "ask_user_params"}}],
            "navigation": [],
            "used": {"mode": "router:report"},
        }

    # 2) crud
    if _detect_crud_intent(msg) and role != "viewer":
        return {
            "answer": "Je peux t’aider à faire cette action. Donne les champs et tu confirmeras avant enregistrement.",
            "sources": [],
            "suggested_actions": [{"type": "crud", "payload": {"name": "draft_action_from_user"}}],
            "navigation": [],
            "used": {"mode": "router:crud"},
        }

    # 3) définitions générales (sans doc)
    if _detect_definition_intent(msg) and re.search(r"\b(fonds|fond)\b", msg, re.I):
        return {
            "answer": "Un fonds d’investissement est un véhicule qui collecte l’argent de souscripteurs pour l’investir selon une stratégie, avec des règles (durée, frais, ratios) et du reporting.",
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {"mode": "router:definition"},
        }

    # 4) KPI déterministe
    kpi_name = route_kpi(msg)
    if kpi_name:
        res = run_kpi(msg)  # IMPORTANT: si None => fallback
        if res is not None:
            return {
                "answer": render_kpi_answer(res),
                "sources": [],
                "suggested_actions": [],
                "navigation": [],
                "used": {"mode": "router:kpi", "kpi": kpi_name, "year": res.get("year"), "debug": debug, "kpi_debug": res.get("debug", {}) if debug else {}},
            }

    # 5) Relational QA (FK joins auto) => 1 phrase
    rel = relational_answer_one_line(msg)
    if rel:
        return {
            "answer": rel,
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": {"mode": "router:relational_qa"},
        }

    # 6) mode agent (si tu veux)
    if mode == "agent" and run_agent is not None:
        res = run_agent(msg, model=model, role=role)
        return {
            "answer": (res.get("text") or "Je ne sais pas d’après les données disponibles.").strip(),
            "sources": [],
            "suggested_actions": res.get("actions", []) or [],
            "navigation": res.get("navigation", []) or [],
            "used": {"mode": "agent-tools", "model": model, "role": role},
        }

    # 7) RAG normal
    chunks, scope, domain = hybrid_retrieve(msg, top_k=top_k)
    sources = _sources_from_chunks(chunks)
    navigation = _suggest_navigation(chunks)

    structured = structured_answer_from_chunks(msg, chunks)
    if structured:
        return {
            "answer": structured,
            "sources": sources,
            "suggested_actions": [],
            "navigation": navigation,
            "used": {"mode": "structured-first", "domain": domain, "scope": scope, "top_k": top_k, "debug": debug},
        }

    llm_text = lc_rag_answer(msg, top_k=top_k, model=model)
    return {
        "answer": llm_text,
        "sources": sources,
        "suggested_actions": [],
        "navigation": navigation,
        "used": {"mode": "llm-fallback", "domain": domain, "scope": scope, "top_k": top_k, "model": model, "debug": debug},
    }
