from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables from .env file
def load_env_file():
    """Load .env file manually if python-dotenv is not available"""
    env_path = Path('.env')
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    load_env_file()

from src.application.config.simple_config import ApplicationConfig


GROUND_TRUTH_PATH = Path("evaluation/ground_truth.json")


def _normalize_connection_uri(uri: str) -> str:
    """Normalize SQLAlchemy connection URI for PostgreSQL."""

    if uri.startswith("postgresql+psycopg2://"):
        return uri.replace("postgresql+psycopg2://", "postgresql://", 1)
    return uri


def _load_ground_truth() -> Dict[str, Dict[str, Any]]:
    with GROUND_TRUTH_PATH.open("r", encoding="utf-8") as f:
        items = json.load(f)
    return {entry["id"]: entry for entry in items}


def _execute_query(engine: Engine, query: Optional[str], limit_rows: Optional[int]) -> Any:
    """Execute SQL and return rows as plain lists. Returns None if query empty."""

    if not query:
        return None

    sql = text(query)

    try:
        with engine.connect() as conn:
            result = conn.execute(sql)

            rows = []
            if limit_rows is None:
                fetched = result.fetchall()
            else:
                fetched = result.fetchmany(limit_rows)

            for row in fetched:
                rows.append([_serialize_cell(value) for value in row])

            return rows

    except SQLAlchemyError as exc:
        return {"error": str(exc)}


def _serialize_cell(value: Any) -> Any:
    """Convert DB cell values to JSON-serializable primitives."""

    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value

    # Convert decimal, datetime, etc. to string for stability
    try:
        return value.isoformat()  # type: ignore[attr-defined]
    except AttributeError:
        return str(value)


def _augment_results(
    results_path: Path,
    output_path: Path,
    limit_rows: Optional[int],
    engine: Engine,
) -> None:
    ground_truth_map = _load_ground_truth()

    with results_path.open("r", encoding="utf-8") as f:
        results = json.load(f)

    for entry in results:
        identifier = entry.get("id")
        gt_entry = ground_truth_map.get(identifier, {})

        gt_query = gt_entry.get("query")
        generated_query = entry.get("generated_sql")

        entry["ground_truth_result"] = _execute_query(engine, gt_query, limit_rows)
        entry["generated_result"] = _execute_query(engine, generated_query, limit_rows)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def _build_engine(config: ApplicationConfig) -> Engine:
    if not config.database_path:
        raise RuntimeError("DATABASE_URL/DATABASE_PATH não configurado no ambiente.")

    connection_uri = _normalize_connection_uri(config.database_path)
    return create_engine(connection_uri)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute SQL and enrich evaluation results with outputs.")
    parser.add_argument(
        "--results-file",
        type=Path,
        required=True,
        help="Path to the tables_result JSON produced by the evaluation script.",
        # Example: --results-file evaluation/results/tables_result_15.json
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Destination path for the enriched JSON. Defaults to <results-file stem>_outputs.json.",
        # Example: --output-file evaluation/results/tables_result_15_enriched.json
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=500,
        help="Maximum rows to fetch per query (default: 500). Use 0 to fetch all rows.",
        # Example: --limit-rows 100 (or --limit-rows 0 for unlimited)
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    limit_rows = None if args.limit_rows == 0 else args.limit_rows
    results_path: Path = args.results_file
    output_path: Path = args.output_file or results_path.with_name(f"{results_path.stem}_outputs.json")

    config = ApplicationConfig()
    engine = _build_engine(config)

    _augment_results(results_path, output_path, limit_rows, engine)

    print(f"[evaluation] Enriched results saved to {output_path}")


if __name__ == "__main__":
    main()
