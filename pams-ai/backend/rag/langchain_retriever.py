from __future__ import annotations

from typing import List, Optional
from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda

from rag.retrieve_core import hybrid_retrieve, extract_entity_hint


def _chunks_to_docs(chunks, top_k: int) -> List[Document]:
    docs: List[Document] = []
    for ch in chunks[:top_k]:
        md = ch.get("metadata") or {}
        docs.append(
            Document(
                page_content=ch.get("content", ""),
                metadata={
                    "id": ch.get("id"),
                    "score": float(ch.get("score") or 0.0),
                    "source_type": ch.get("source_type"),
                    "source_id": ch.get("source_id"),
                    **md,
                },
            )
        )
    return docs


def _retrieve_docs(query: str, top_k: int = 8, source_types: Optional[List[str]] = None) -> List[Document]:
    hint = extract_entity_hint(query)

    chunks, scope, domain = hybrid_retrieve(
        query,
        top_k=top_k,
        source_types=source_types,   
        entity_hint=hint,
    )

    docs = _chunks_to_docs(chunks, top_k=top_k)
   
    for d in docs:
        d.metadata["_domain"] = domain
        d.metadata["_scope"] = scope
    return docs


def make_retriever(top_k: int = 8, source_types: Optional[List[str]] = None):
    """
    Retourne un "retriever" compatible LangChain (Runnable).
    - invoke(question) -> List[Document]
    """
    return RunnableLambda(lambda q: _retrieve_docs(q, top_k=top_k, source_types=source_types))
