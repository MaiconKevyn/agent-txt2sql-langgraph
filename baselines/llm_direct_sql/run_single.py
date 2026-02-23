from __future__ import annotations

import argparse
import json

from baselines.llm_direct_sql.config import BaselineConfig
from baselines.llm_direct_sql.pipeline import run_single_question


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run direct LLM baseline for a single question (no LangGraph)."
    )
    parser.add_argument("--question", required=True, help="Natural language question")
    parser.add_argument("--provider", help="LLM provider: ollama, openai, groq")
    parser.add_argument("--model", help="LLM model name")
    parser.add_argument("--temperature", type=float, help="LLM temperature")
    parser.add_argument("--timeout", type=int, help="LLM timeout in seconds")
    parser.add_argument("--db-url", help="PostgreSQL URL")
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        help="PostgreSQL statement timeout in milliseconds",
    )
    parser.add_argument(
        "--include-raw-response",
        action="store_true",
        help="Persist raw LLM response in output JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = BaselineConfig.from_env(
        llm_provider=args.provider,
        llm_model=args.model,
        llm_temperature=args.temperature,
        llm_timeout=args.timeout,
        database_url=args.db_url,
        statement_timeout_ms=args.statement_timeout_ms,
        include_raw_response=args.include_raw_response,
    )

    result = run_single_question(args.question, config)

    print(f"Provider/Model: {config.llm_provider} / {config.llm_model}")
    print(f"Safe SQL: {result['is_safe_sql']}")
    if not result["is_safe_sql"]:
        print(f"Safety reason: {result['safety_reason']}")
    print("\nGenerated SQL:")
    print(result["predicted_sql"] or result["unsafe_predicted_sql"] or "<empty>")
    print("\nExecution:")
    print(f"  Row count: {result['execution']['row_count']}")
    print(f"  Error: {result['execution']['error']}")
    print(f"  LLM latency (s): {result['llm_latency_s']:.3f}")
    print(f"  SQL latency (s): {result['execution']['latency_s']:.3f}")
    print("  Rows preview:")
    print(json.dumps(result["execution"]["rows_preview"], ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

