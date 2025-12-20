import argparse
import logging
import os

from .agent import LangGraphChatAgent
from .config import load_config
from .db import Database


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refactored TXT2SQL Agent")
    parser.add_argument("--query", type=str, help="Natural language query")
    parser.add_argument("--db-url", type=str, help="PostgreSQL connection URL")
    parser.add_argument("--provider", type=str, default=None, help="LLM provider (ollama|huggingface)")
    parser.add_argument("--model", type=str, default=None, help="LLM model name")
    parser.add_argument("--temperature", type=float, default=None, help="LLM temperature")
    parser.add_argument("--timeout", type=int, default=None, help="LLM timeout seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print config and exit")
    parser.add_argument("--self-test", action="store_true", help="Test DB connection and exit")
    parser.add_argument("--interactive", action="store_true", help="Start interactive chat session")
    parser.add_argument("--session-id", type=str, default="chat", help="Session id for chat memory")
    return parser.parse_args()


def _resolve_db_url(args: argparse.Namespace) -> str:
    return args.db_url or os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH") or ""


def main() -> int:
    logging.basicConfig(level=logging.WARNING)  # Reduz logs para não poluir saída
    args = _parse_args()

    if args.dry_run:
        db_url = _resolve_db_url(args)
        print("Dry run mode")
        print(f"DB URL configured: {bool(db_url)}")
        print(f"Provider: {args.provider or os.getenv('LLM_PROVIDER', 'ollama')}")
        print(f"Model: {args.model or os.getenv('LLM_MODEL', 'llama3.1:8b')}")
        return 0

    db_url = _resolve_db_url(args)
    if not db_url:
        print("DATABASE_URL not set. Use --db-url or set env.")
        return 1

    if args.self_test:
        db = Database(db_url)
        ok = db.ping()
        print("DB connection ok" if ok else "DB connection failed")
        return 0 if ok else 2

    config = load_config(
        database_url=db_url,
        llm_provider=args.provider,
        llm_model=args.model,
        llm_temperature=args.temperature,
        llm_timeout=args.timeout,
    )

    agent = LangGraphChatAgent(config)

    if args.interactive:
        print("TXT2SQL Chat - digite 'sair' para encerrar.")
        if args.query:
            result = agent.ask(args.query, session_id=args.session_id)
            _print_result(result)
        while True:
            try:
                user_input = input("> ").strip()
            except EOFError:
                break
            if not user_input:
                continue
            if user_input.lower() in {"sair", "exit", "quit"}:
                break
            result = agent.ask(user_input, session_id=args.session_id)
            _print_result(result)
        return 0

    if not args.query:
        print("Missing --query (or use --interactive)")
        return 1

    result = agent.ask(args.query, session_id=args.session_id)
    _print_result(result)
    return 0 if result.get("success") else 2


def _print_result(result: dict, show_sql: bool = True) -> None:
    print()
    print(result.get("response", ""))
    if show_sql and result.get("sql"):
        print()
        print(f"[SQL] {result['sql']}")
    if result.get("error"):
        print(f"[ERRO] {result['error']}")


if __name__ == "__main__":
    raise SystemExit(main())
