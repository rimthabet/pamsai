from typing import Any, Dict, List, Optional, Tuple
import re
import time
import logging

from rag.retrieve_core import hybrid_retrieve
from rag.answer import structured_answer_from_chunks
from rag.langchain_answer import answer as lc_rag_answer
from agent.lc_agent import run_agent

from tools.analytics_parser import parse_intent
from tools.analytics_service import run_analytics
from tools.sql_ai_engine import try_answer_sql

log = logging.getLogger(__name__)

RX_DATE_3112 = re.compile(r"\b31\s*/\s*12\s*/\s*(19\d{2}|20\d{2})\b", re.I)
RX_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RX_ACTIF_NET = re.compile(
    r"\bactif\s+net\b.{0,80}?(?:\bde\b|:|=)?\s*([0-9][0-9\.\s,]*?)\s*(?:DT|D\.T|TND)\b",
    re.I,
)
RX_VL = re.compile(
    r"\bvaleur\s+liquidative\b.{0,80}?(?:égale\s+à|egal[e]?\s+a|:|=)?\s*([0-9][0-9\.\s,]*?)\s*(?:DT|D\.T|TND)\b",
    re.I,
)


def _sources_from_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ch in chunks:
        out.append(
            {
                "id": int(ch.get("id")),
                "source_type": ch.get("source_type") or "",
                "source_id": str(ch.get("source_id") or ""),
                "score": float(ch.get("score") or 0.0),
                "metadata": ch.get("metadata") or {},
            }
        )
    return out


def _suggest_navigation(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    nav: List[Dict[str, Any]] = []
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


def _parse_dt_number(raw: str) -> Optional[float]:
    if not raw:
        return None
    s = raw.strip().replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    if "," in s:
        s = s.replace(" ", "").replace(".", "")
        s = s.replace(",", ".")
    else:
        s = s.replace(" ", "").replace(".", "")
    try:
        return float(s)
    except Exception:
        return None


def _fmt_dt(v: Optional[float], decimals: int = 3) -> str:
    if v is None:
        return ""
    if v >= 10000:
        return f"{v:,.0f}".replace(",", " ") + " DT"
    return f"{v:,.{decimals}f}".replace(",", " ").replace(".", ",") + " DT"


def extract_financials_from_ocr(query: str, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    q = query or ""
    year_wanted: Optional[int] = None
    m = RX_YEAR.search(q)
    if m:
        try:
            year_wanted = int(m.group(1))
        except Exception:
            year_wanted = None

    best: Dict[str, Any] = {
        "year": None,
        "actif_net": None,
        "vl": None,
        "file": None,
        "page": None,
        "score": 0.0,
    }

    for ch in chunks:
        if ch.get("source_type") != "pdf_ocr":
            continue

        meta = ch.get("metadata") or {}
        content = ch.get("content") or ""
        score = float(ch.get("score") or 0.0)

        chunk_year: Optional[int] = None
        m1 = RX_DATE_3112.search(content)
        if m1:
            chunk_year = int(m1.group(1))
        else:
            m2 = RX_YEAR.search(content)
            if m2:
                chunk_year = int(m2.group(1))

        if year_wanted is not None and chunk_year is not None and chunk_year != year_wanted:
            continue

        actif = None
        vl = None

        ma = RX_ACTIF_NET.search(content)
        if ma:
            actif = _parse_dt_number(ma.group(1))

        mv = RX_VL.search(content)
        if mv:
            vl = _parse_dt_number(mv.group(1))

        if actif is None and vl is None:
            continue

        bonus = 0.05 if (actif is not None and vl is not None) else 0.0
        final_score = score + bonus

        if final_score > float(best["score"]):
            best.update(
                {
                    "year": chunk_year,
                    "actif_net": actif,
                    "vl": vl,
                    "file": meta.get("file") or ch.get("source_id"),
                    "page": meta.get("page"),
                    "score": final_score,
                }
            )

    return best


def _render_document_answer(message: str, chunks: List[Dict[str, Any]], max_files: int = 5) -> str:
    pdf_hits: List[Tuple[str, int, float, str]] = []
    q_l = (message or "").lower()
    must_match_fonds = ("croissance" in q_l and "entreprises" in q_l)

    for ch in chunks:
        if ch.get("source_type") != "pdf_ocr":
            continue
        meta = ch.get("metadata") or {}
        fname = meta.get("file") or ch.get("source_id") or "pdf"
        page = int(meta.get("page") or 0)
        score = float(ch.get("score") or 0.0)
        content = (ch.get("content") or "").strip()

        if must_match_fonds:
            c_l = content.lower()
            f_l = str(fname).lower()
            if (("croissance" not in c_l and "croissance" not in f_l) or ("entreprises" not in c_l and "entreprises" not in f_l)):
                continue

        pdf_hits.append((str(fname), page, score, content))

    if not pdf_hits:
        return ""

    best_by_file: Dict[str, Tuple[int, float, str]] = {}
    for fname, page, score, content in pdf_hits:
        cur = best_by_file.get(fname)
        if cur is None or score > cur[1]:
            best_by_file[fname] = (page, score, content)

    items = sorted(best_by_file.items(), key=lambda kv: kv[1][1], reverse=True)[:max_files]

    lines: List[str] = []
    lines.append("J'ai trouve la reponse dans ces documents OCR :\n")
    for i, (fname, (page, score, content)) in enumerate(items, start=1):
        snippet = " ".join(content.split())
        snippet = snippet[:240] + ("..." if len(snippet) > 240 else "")
        ptxt = f" (page {page})" if page else ""
        lines.append(f"{i}. **{fname}**{ptxt} - score {score:.2f}")
        lines.append(f"   Extrait : {snippet}")

    lines.append("\nDis-moi l'annee (ex: 2017, 2019, 2023) et je te sors: actif net + valeur liquidative.")
    return "\n".join(lines)


def _format_context(chunks: List[Dict[str, Any]], max_chars: int = 4500) -> str:
    parts: List[str] = []
    total = 0
    for ch in chunks:
        st = ch.get("source_type") or ""
        meta = ch.get("metadata") or {}
        if st == "pdf_ocr":
            header = f"[pdf={meta.get('file','')} page={meta.get('page','')}]"
        else:
            header = f"[{st} {ch.get('source_id','')}]"

        txt = (ch.get("content") or "").strip()
        if not txt:
            continue
        block = f"{header}\n{txt}\n"
        if total + len(block) > max_chars:
            break
        parts.append(block)
        total += len(block)
    return "\n".join(parts)


def _lc_answer_with_chunks(message: str, chunks: List[Dict[str, Any]], top_k: int, model: str) -> str:
    ctx = _format_context(chunks)
    if ctx:
        enriched = (
            "Tu es un assistant RAG. Reponds uniquement avec les informations du CONTEXTE.\n"
            "Si l'info n'est pas dans le contexte, dis 'Je ne sais pas'.\n\n"
            f"QUESTION:\n{message}\n\n"
            f"CONTEXTE:\n{ctx}\n"
        )
        return lc_rag_answer(enriched, top_k=top_k, model=model)
    return lc_rag_answer(message, top_k=top_k, model=model)


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
            "answer": "Je peux generer un reporting. Dis-moi le fonds/periode et le format (PDF/Excel).",
            "sources": [],
            "suggested_actions": [{"type": "report", "payload": {"name": "ask_user_params"}}],
            "navigation": [],
            "used": {"mode": "router:report_hint", "debug": debug},
        }

    if _detect_crud_intent(message) and role != "viewer":
        return {
            "answer": "Je peux t'aider a faire cette action. Donne les champs et tu confirmeras avant enregistrement.",
            "sources": [],
            "suggested_actions": [{"type": "crud", "payload": {"name": "draft_action_from_user"}}],
            "navigation": [],
            "used": {"mode": "router:crud_hint", "debug": debug},
        }

    if mode == "agent":
        res = run_agent(message, model=model, role=role)
        text = res.get("text") or "Je ne sais pas d'apres les donnees disponibles."
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

    t0 = time.time()
    log.warning("STEP 1: try_answer_sql start")
    s = try_answer_sql(message, debug=debug)
    log.warning("STEP 1 done in %.2fs", time.time() - t0)

    if s and s.get("ok"):
        return {
            "answer": s["text"],
            "sources": [],
            "suggested_actions": [],
            "navigation": [],
            "used": s.get("used") or {"mode": "sql", "debug": debug},
        }

    t1 = time.time()
    log.warning("STEP 2: hybrid_retrieve start")
    chunks, scope, domain = hybrid_retrieve(message, top_k=top_k)
    log.warning("STEP 2 done in %.2fs", time.time() - t1)

    sources = _sources_from_chunks(chunks)
    navigation = _suggest_navigation(chunks)

    if domain == "document":
        fin = extract_financials_from_ocr(message, chunks)
        doc_text = _render_document_answer(message, chunks)

        if doc_text:
            lines: List[str] = []
            if fin.get("actif_net") is not None or fin.get("vl") is not None:
                y = fin.get("year")
                header = f"Chiffres cles au 31/12/{y} :" if y else "Chiffres cles trouves :"
                lines.append(header)
                if fin.get("actif_net") is not None:
                    lines.append(f"- Actif net: {_fmt_dt(fin['actif_net'])}")
                if fin.get("vl") is not None:
                    lines.append(f"- Valeur liquidative: {_fmt_dt(fin['vl'])} par part")
                if fin.get("file"):
                    ptxt = f" (page {fin.get('page')})" if fin.get("page") else ""
                    lines.append(f"Source: {fin['file']}{ptxt}")
                lines.append("")

            answer = ("\n".join(lines) + "\n" if lines else "") + doc_text

            return {
                "answer": answer,
                "sources": sources,
                "suggested_actions": [],
                "navigation": navigation,
                "used": {"mode": "doc-render+extract", "domain": domain, "scope": scope, "top_k": top_k, "debug": debug},
            }

    structured = structured_answer_from_chunks(message, chunks)
    if structured:
        return {
            "answer": structured,
            "sources": sources,
            "suggested_actions": [],
            "navigation": navigation,
            "used": {"mode": "structured-first", "domain": domain, "scope": scope, "top_k": top_k, "debug": debug},
        }

    t2 = time.time()
    log.warning("STEP 4: llm answer start")
    llm_text = _lc_answer_with_chunks(message, chunks, top_k=top_k, model=model)
    log.warning("STEP 4 done in %.2fs", time.time() - t2)

    return {
        "answer": llm_text,
        "sources": sources,
        "suggested_actions": [],
        "navigation": navigation,
        "used": {"mode": "llm-fallback-with-context", "domain": domain, "scope": scope, "top_k": top_k, "model": model, "debug": debug},
    }
