"""Run table-selection evaluation against ground-truth prompts.

This script iterates through the entries in ``evaluation/ground_truth.json``,
invokes the current LangGraph workflow for each question, captures the tables
selected by the agent, and writes the outcomes to a timestamped JSON file under
``evaluation/results``.

Each result object contains:
    - ``id``: ground-truth identifier
    - ``question``: natural language prompt
    - ``ground_truth_tables``: expected tables from the dataset
    - ``selected_tables``: tables chosen by the agent's table-selection node
    - ``tables_match``: boolean flag comparing selected vs expected tables
    - ``generated_sql``: SQL produced by the agent (validated when available)
    - ``timestamp``: ISO-8601 timestamp of the evaluation run

Usage:
    python evaluation/scripts/run_table_selection_eval.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(dotenv_path=REPO_ROOT / ".env")

from src.agent.state import create_initial_messages_state
from src.agent.workflow import create_sql_agent_workflow


RESULTS_DIR = Path("evaluation/results")
GROUND_TRUTH_PATH = Path("evaluation/ground_truth.json")


def _next_results_path() -> Path:
    """Compute the next ``tables_result_<n>.json`` path."""

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    prefix = "tables_result_"
    existing: List[int] = []

    for file in RESULTS_DIR.glob(f"{prefix}*.json"):
        try:
            suffix = file.stem.replace(prefix, "")
            existing.append(int(suffix))
        except ValueError:
            continue

    next_index = max(existing, default=0) + 1
    return RESULTS_DIR / f"{prefix}{next_index}.json"

def _ensure_database_available(connection_uri: str) -> None:
    """Fail fast with a helpful message if the database is unreachable."""

    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError

    if connection_uri.startswith("postgresql+psycopg2://"):
        connection_uri = connection_uri.replace("postgresql+psycopg2://", "postgresql://", 1)

    engine = create_engine(connection_uri)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"[evaluation] Database connection successful: {connection_uri}")
    except SQLAlchemyError as exc:  # pragma: no cover - defensive
        print(f"[evaluation] Database connection failed: {exc}")
        raise RuntimeError(
            "DB_UNAVAILABLE"
        ) from exc


# Offline mode removed - always use database connection


def _load_ground_truth() -> List[Dict[str, Any]]:
    with GROUND_TRUTH_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def evaluate_table_selection() -> List[Dict[str, Any]]:
    """Run the workflow over ground-truth questions and collect table picks."""

    from src.application.config.simple_config import ApplicationConfig
    from src.agent import nodes as agent_nodes

    config = ApplicationConfig()
    
    # Always try to use online mode first
    print(f"[evaluation] Attempting database connection: {config.database_path}")
    
    if not config.database_path:
        print("[evaluation] No database path configured, skipping online mode.")
        online = False
    else:
        try:
            _ensure_database_available(config.database_path)
            online = True
            print("[evaluation] Running in ONLINE mode with database connection.")
        except RuntimeError as e:
            print(f"[evaluation] Database unavailable ({e}), skipping online mode.")
            online = False

    if not online:
        print("[evaluation] Database unavailable; this evaluation requires a working database.")
        print("[evaluation] Please ensure PostgreSQL is running and credentials are correct.")
        return []

    # Create workflow for online evaluation
    workflow = create_sql_agent_workflow()
    ground_truth = _load_ground_truth()
    results: List[Dict[str, Any]] = []

    # For testing, limit to first 10 cases for faster evaluation
    test_cases = ground_truth[:70] if len(ground_truth) > 10 else ground_truth
    print(f"[evaluation] Processing {len(test_cases)} test cases (limited for performance)...")

    for i, entry in enumerate(test_cases, 1):
        question = entry.get("question", "")
        identifier = entry.get("id", "")

        print(f"[evaluation] Processing {i}/{len(test_cases)}: {identifier} - {question[:50]}...")

        session_id = f"eval_{identifier or hash(question)}"
        initial_state = create_initial_messages_state(user_query=question, session_id=session_id)

        try:
            # For evaluation, we only need to run until table selection
            # Run: classify -> list_tables -> get_schema (tables should be selected by then)
            st = initial_state
            st = agent_nodes.query_classification_node(st)
            
            # Only run table selection if we need DATABASE or SCHEMA route
            if st.get("query_route") and st["query_route"].value in ["database", "schema"]:
                st = agent_nodes.list_tables_node(st)
                # Optional: get schema to see full behavior
                try:
                    st = agent_nodes.get_schema_node(st)
                except Exception as e:
                    print(f"[evaluation] Schema node failed for {identifier}: {e}")
            
            final_state = st
            selected_tables = list(final_state.get("selected_tables", []))
            print(f"[evaluation] Selected tables for {identifier}: {selected_tables}")
            
        except Exception as e:
            print(f"[evaluation] Error processing {identifier}: {e}")
            final_state = initial_state
            selected_tables = []

        ground_truth_tables = list(entry.get("tables", []))
        
        # NEW LOGIC: Check if ALL ground truth tables are present in selected tables (RECALL)
        # We don't care about precision (extra tables), only that we captured all required ones
        ground_truth_set = set(ground_truth_tables)
        selected_set = set(selected_tables)
        tables_match = ground_truth_set.issubset(selected_set)
        
        # Calculate recall and precision for detailed analysis
        recall = len(ground_truth_set.intersection(selected_set)) / len(ground_truth_set) if ground_truth_set else 1.0
        precision = len(ground_truth_set.intersection(selected_set)) / len(selected_set) if selected_set else 0.0

        results.append(
            {
                "id": identifier,
                "question": question,
                "ground_truth_tables": ground_truth_tables,
                "selected_tables": selected_tables,
                "tables_match": tables_match,
                "recall": recall,
                "precision": precision,
                "generated_sql": None,  # Not needed for table selection evaluation
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    return results


def main() -> None:
    results = evaluate_table_selection()
    output_path = _next_results_path()

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved table-selection evaluation to {output_path}")


if __name__ == "__main__":
    main()
