import argparse
from app.chat_service import chat_pipeline

def main():
    p = argparse.ArgumentParser()
    p.add_argument("message", type=str)
    p.add_argument("-k", "--top_k", type=int, default=8)
    p.add_argument("--model", type=str, default="llama3.2")
    p.add_argument("--role", type=str, default="viewer")
    p.add_argument("--mode", type=str, default="rag", help="rag|agent")
    args = p.parse_args()

    out = chat_pipeline(
        message=args.message,
        top_k=args.top_k,
        model=args.model,
        role=args.role,
        mode=args.mode,
    )
    print("\nANSWER:\n", out["answer"])
    if out.get("used"):
        print("\nUSED:", out["used"])
    if out.get("navigation"):
        print("\nNAV:", out["navigation"])
    if out.get("sources"):
        print("\nSOURCES:")
        for s in out["sources"][:5]:
            print(" -", s)

if __name__ == "__main__":
    main()
