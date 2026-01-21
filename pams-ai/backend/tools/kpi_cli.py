import argparse
import json

from tools.kpi_service import run_kpi


def main():
    p = argparse.ArgumentParser(description="KPI CLI")
    p.add_argument("message", type=str)
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()

    res = run_kpi(args.message, debug=args.debug)
    if not res:
        print("NO KPI MATCH")
        return

    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
