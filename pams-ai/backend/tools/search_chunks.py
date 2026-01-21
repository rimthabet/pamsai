from __future__ import annotations

import argparse
from typing import Any, Dict, List, Optional

from core.policy import get_policy, clamp_top_k, filter_source_types, enforce_tool
from rag.retrieve_core import hybrid_retrieve


def search_chunks(
    query: str,
    top_k: int = 8,
    role: str = "analyst",
    source_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Tool stable: retrieval hybride (keyword + pgvector) avec policy.
    Retourne chunks + scope + domain.
    """
    policy = get_policy(role)
    enforce_tool("search_chunks", policy)

    k = clamp_top_k(top_k, policy)
    sts = filter_source_types(source_types, policy)

    chunks, scope, domain = hybrid_retrieve(query, top_k=k)  
    if sts is not None:
        chunks = [c for c in chunks if c.get("source_type") in set(sts)]
        chunks = chunks[:k]
        scope = sts

    # output normalisé
    out_chunks = []
    for c in chunks:
        out_chunks.append({
            "id": int(c.get("id")),
            "source_type": c.get("source_type") or "",
            "source_id": str(c.get("source_id") or ""),
            "score": float(c.get("score") or 0.0),
            "metadata": c.get("metadata") or {},
            "content": c.get("content") or "",
        })

    return {
        "query": query,
        "top_k": k,
        "domain": domain,
        "scope": scope,
        "chunks": out_chunks,
        "used": {"tool": "search_chunks", "role": policy.role, "filtered_source_types": sts},
    }


def _cli():
    p = argparse.ArgumentParser(description="Tool: search_chunks (hybrid retrieve)")
    p.add_argument("query", type=str)
    p.add_argument("-k", "--top_k", type=int, default=8)
    p.add_argument("--role", type=str, default="analyst")
    p.add_argument("--source_type", action="append", default=[], help="Repeatable. ex: --source_type maxula:fonds")
    args = p.parse_args()

    res = search_chunks(
        args.query,
        top_k=args.top_k,
        role=args.role,
        source_types=args.source_type or None,
    )

    print("\n====================================")
    print("QUERY:", res["query"])
    print("TOP_K:", res["top_k"])
    print("DOMAIN:", res["domain"])
    print("SCOPE:", res["scope"])
    print("====================================\n")

    if not res["chunks"]:
        print("Aucun chunk.")
        return

    for i, ch in enumerate(res["chunks"], start=1):
        preview = (ch["content"] or "")[:500].replace("\n", " ")
        print(f"[{i}] score={ch['score']:.4f} | {ch['source_type']} | {ch['source_id']} | id={ch['id']}")
        print("metadata:", ch["metadata"])
        print("preview:", preview)
        print("-" * 60)


if __name__ == "__main__":
    _cli()
