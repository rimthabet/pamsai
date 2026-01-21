import argparse
from rag.langchain_answer import answer

def main():
    p = argparse.ArgumentParser()
    p.add_argument("question", type=str)
    p.add_argument("-k", "--top_k", type=int, default=8)
    p.add_argument("--model", type=str, default="llama3.2")
    args = p.parse_args()

    print(answer(args.question, top_k=args.top_k, model=args.model))

if __name__ == "__main__":
    main()
