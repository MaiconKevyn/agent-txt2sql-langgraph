"""
Entrypoint for the Rich Prompt Baseline.

Runs a single-shot LLM evaluation with full context (same as LangGraph agent)
but without LangGraph architecture (no table selection, validation, or repair).

Experimental purpose: isolate the impact of LangGraph architecture vs prompt engineering.

Usage:
    python -m baselines.rich_prompt_baseline.run_batch
    python evaluation/run_rich_prompt_baseline.py

Config via environment variables (or .env):
    OPENAI_API_KEY        — required
    DATABASE_URL          — required (PostgreSQL connection string)
    BASELINE_LLM_MODEL    — default: gpt-4o-mini
    BASELINE_LLM_TEMPERATURE — default: 0
"""
from __future__ import annotations

import sys

from baselines.rich_prompt_baseline.config import BaselineConfig
from baselines.rich_prompt_baseline.pipeline import (
    default_output_path,
    load_ground_truth,
    run_batch,
    save_results,
)

GROUND_TRUTH_PATH = "evaluation/ground_truth.json"


def main() -> int:
    config = BaselineConfig.from_env(
        llm_model="gpt-4o-mini",
        output_dir="baselines/rich_prompt_baseline/artifacts",
    )

    print("=" * 70)
    print("RICH PROMPT BASELINE — single-shot LLM, full context, no LangGraph")
    print("=" * 70)
    print(f"Model   : openai/{config.llm_model}")
    print(f"DB      : {config.database_url[:40]}...")
    print(f"Timeout : {config.statement_timeout_ms}ms")
    print()

    questions = load_ground_truth(GROUND_TRUTH_PATH)
    print(f"Loaded {len(questions)} questions from {GROUND_TRUTH_PATH}")
    print()

    results = run_batch(questions, config)

    out = save_results(results, default_output_path(config.output_dir))

    # Summary
    metrics = results.get("metrics", {})
    ex = metrics.get("Execution Accuracy (EX)", {})
    em = metrics.get("Exact Match (EM)", {})
    cm = metrics.get("Component Matching (CM)", {})

    print()
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    print(f"Total questions : {results['total_questions']}")
    print(f"EX (Execution Accuracy) : {ex.get('average_score', 0):.4f}  "
          f"({ex.get('perfect_matches', 0)}/{ex.get('total_evaluated', 0)} correct)")
    print(f"EM (Exact Match)        : {em.get('average_score', 0):.4f}  "
          f"({em.get('perfect_matches', 0)}/{em.get('total_evaluated', 0)} correct)")
    print(f"CM (Component Matching) : {cm.get('average_score', 0):.4f}")
    print()
    print(f"Results saved to: {out}")
    print()
    print("Comparison reference:")
    print(f"  Rich Prompt Baseline (GPT-4o-mini, single-shot): EX {ex.get('average_score', 0):.1%} — THIS RUN")
    print("  LangGraph Agent      (GPT-4o-mini, full pipeline): EX 96.3%")
    print()
    print("Gap = impact of LangGraph architecture (table selection + validation + repair)")
    print("If gap ≈ 0%  → prompts alone explain all gains; LangGraph adds no value")
    print("If gap > 5%  → LangGraph architecture provides measurable benefit")

    return 0


if __name__ == "__main__":
    sys.exit(main())
