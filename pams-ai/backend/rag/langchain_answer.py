from __future__ import annotations

from typing import Any, Dict, List
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document

from rag.answer import structured_answer_from_chunks
from rag.langchain_retriever import make_retriever
from rag.langchain_llm import get_llm


PROMPT = ChatPromptTemplate.from_messages([
    ("system",
     "Tu es un assistant métier PAMS.\n"
     "Règles STRICTES:\n"
     "1) Réponds uniquement à partir du CONTEXTE fourni.\n"
     "2) Si l'information n'est pas dans le contexte, répond EXACTEMENT: "
     "\"Je ne sais pas d’après les documents disponibles.\".\n"
     "3) Quand tu affirmes un chiffre/valeur/fait, cite au moins une source [Sx].\n"
     "4) Ne devine pas, ne complète pas.\n"),
    ("user",
     "QUESTION: {question}\n\n"
     "CONTEXTE:\n{context}\n\n"
     "Réponds en français, court et précis.\n"
     "RÉPONSE:")
])


def docs_to_context(docs: List[Document], max_chars_per_doc: int = 1600) -> str:
    blocks = []
    for i, d in enumerate(docs, start=1):
        md = d.metadata or {}
        blocks.append(
            f"[S{i}] score={md.get('score')} "
            f"source_type={md.get('source_type')} source_id={md.get('source_id')} id={md.get('id')}\n"
            f"{(d.page_content or '')[:max_chars_per_doc]}"
        )
    return "\n\n".join(blocks)


def docs_to_chunks(docs: List[Document]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in docs:
        md = d.metadata or {}
        out.append({
            "id": md.get("id"),
            "score": float(md.get("score") or 0.0),
            "source_type": md.get("source_type"),
            "source_id": md.get("source_id"),
            "metadata": md,           
            "content": d.page_content,
        })
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def answer(question: str, top_k: int = 8, model: str = "llama3.2") -> str:
    retriever = make_retriever(top_k=top_k)
    docs = retriever.invoke(question)

    chunks = docs_to_chunks(docs)
    sa = structured_answer_from_chunks(question, chunks)
    if sa:
        return sa

    # LLM fallback (RAG)
    llm = get_llm(model=model)
    context = docs_to_context(docs)
    chain = PROMPT | llm
    res = chain.invoke({"question": question, "context": context})
    return (getattr(res, "content", "") or "").strip()
