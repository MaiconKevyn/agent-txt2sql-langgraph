"""
Convenience entrypoint for the direct LLM baseline (no LangGraph).
"""

from baselines.llm_direct_sql.run_batch import main


if __name__ == "__main__":
    raise SystemExit(main())

