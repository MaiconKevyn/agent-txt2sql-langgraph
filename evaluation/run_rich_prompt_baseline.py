"""
Convenience entrypoint for the Rich Prompt Baseline (no LangGraph).

Same context as the LangGraph agent (RULES A-H + TABLE_TEMPLATES + SUS_MAPPINGS),
but as a single-shot direct OpenAI API call without the graph pipeline.

Experimental purpose: isolate the impact of LangGraph architecture vs prompt engineering.

Usage:
    python evaluation/run_rich_prompt_baseline.py
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from baselines.rich_prompt_baseline.run_batch import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
