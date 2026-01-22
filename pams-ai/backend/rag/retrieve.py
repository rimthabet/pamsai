import argparse
from rag.retrieve_core import hybrid_retrieve, auto_source_types, semantic_retrieve

def main():
    p = argparse.ArgumentParser()
    p.add_argument("query", type=str)
    p.add_argument("-k", "--top_k", type=int, default=10)
    p.add_argument("--source_type", type=str, default="")
    p.add_argument("--no_auto_scope", action="store_true")
    args = p.parse_args()

    if args.source_type:
        scope = [args.source_type]
        rows = semantic_retrieve(args.query, top_k=args.top_k, source_types=scope)
        domain = ""
    else:
        rows, scope, domain = hybrid_retrieve(args.query, top_k=args.top_k)

    print("\n====================================")
    print("QUERY:", args.query)
    print("TOP_K:", args.top_k)
    print("DOMAIN:", domain if domain else "(unknown)")
    print("AUTO_SCOPE source_types:", scope if scope else "(global)")
    print("====================================\n")

    for i, r in enumerate(rows, start=1):
        print(f"[{i}] score={r['score']:.4f} | {r['source_type']} | id={r['source_id']} | id={r['id']}")
        print("metadata:", r["metadata"])
        print("preview:\n", r["content"][:500])
        print("-" * 60)

if __name__ == "__main__":
    main()
