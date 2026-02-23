from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Any

from baselines.llm_direct_sql.config import BaselineConfig
from baselines.llm_direct_sql.pipeline import (
    default_output_path,
    load_ground_truth,
    run_batch,
    save_results,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run direct LLM baseline in batch mode (no LangGraph)."
    )
    parser.add_argument(
        "--ground-truth",
        default="evaluation/ground_truth.json",
        help="Path to ground truth JSON",
    )
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
        "--difficulty",
        action="append",
        help="Filter difficulty (can repeat): easy, medium, hard",
    )
    parser.add_argument(
        "--max-questions",
        type=int,
        help="Run only first N questions after filtering",
    )
    parser.add_argument(
        "--output",
        help="Output JSON file path. Default: baselines/llm_direct_sql/artifacts/...",
    )
    parser.add_argument(
        "--include-raw-response",
        action="store_true",
        help="Persist raw LLM response in output JSON",
    )
    parser.add_argument(
        "--skip-metrics",
        action="store_true",
        help="Skip EM/CM/EX metrics and only run generation+execution",
    )
    return parser.parse_args()


def _filter_questions(
    questions: List[Dict[str, Any]],
    difficulties: List[str] | None,
    max_questions: int | None,
) -> List[Dict[str, Any]]:
    filtered = questions
    if difficulties:
        wanted = {d.lower().strip() for d in difficulties}
        filtered = [q for q in filtered if q.get("difficulty", "").lower() in wanted]
    if max_questions and max_questions > 0:
        filtered = filtered[: max_questions]
    return filtered


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

    questions = load_ground_truth(args.ground_truth)
    selected = _filter_questions(questions, args.difficulty, args.max_questions)
    if not selected:
        raise ValueError("No questions selected after filters")

    print(f"Selected questions: {len(selected)}")
    print(f"Provider/Model: {config.llm_provider} / {config.llm_model}")

    results = run_batch(
        selected,
        config,
        include_metrics=not args.skip_metrics,
    )

    output_path = Path(args.output) if args.output else default_output_path(config.output_dir)
    saved_at = save_results(results, output_path)

    summary = results["summary"]
    print("\nSummary:")
    print(f"  Total questions: {summary['total_questions']}")
    print(f"  Agent success rate: {summary['agent_success_rate']:.2%}")
    print(f"  Unsafe SQL rate: {summary['unsafe_sql_rate']:.2%}")
    print(f"  Execution error rate: {summary['execution_error_rate']:.2%}")
    print(f"  Avg LLM latency (s): {summary['avg_llm_latency_s']:.3f}")
    print(f"  Avg SQL latency (s): {summary['avg_execution_latency_s']:.3f}")
    print(f"\nSaved results: {saved_at}")

    if results.get("metrics"):
        print("\nMetrics:")
        for metric_name, data in results["metrics"].items():
            print(
                f"  {metric_name}: avg={data['average_score']:.3f} "
                f"acc={data['accuracy']:.2%} n={data['total_evaluated']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

