import argparse
import os
from app.db import engine
from tools.schema_cache import build_schema_cache
from tools.analytics_service import run_analytics

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("q", type=str)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    exclude = os.getenv("MAXULA_EXCLUDE_TABLES", "alembic_version,rag_chunks,rag_sources").split(",")
    schema = build_schema_cache(engine, exclude_tables=exclude)

    res = run_analytics(args.q, schema_cache=schema)
    if not res["ok"]:
        print("NO ANALYTICS:", res)
        return
    print(res["answer"])
    if args.debug:
        print("USED:", res["used"])

if __name__ == "__main__":
    main()
