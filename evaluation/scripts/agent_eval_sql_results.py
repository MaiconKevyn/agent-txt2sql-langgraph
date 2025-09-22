"""Evaluate SQL execution outputs against ground truth results."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Counter as CounterType, Iterable, List, Optional, Sequence


@dataclass
class EntryEvaluation:
    identifier: str
    match: bool
    status: str
    ground_truth_rows: int
    generated_rows: Optional[int]
    error: Optional[str] = None
    missing_rows: Optional[List[Sequence[Any]]] = None
    extra_rows: Optional[List[Sequence[Any]]] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare generated SQL results with ground-truth outputs.")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to evaluation results enriched with SQL outputs.")
    parser.add_argument(
        "--max-diff-rows",
        type=int,
        default=5,
        help="Maximum number of differing rows to display per entry (default: 5).",
    )
    return parser.parse_args()


def _load_results(path: Path) -> List[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _rows_to_counter(rows: Iterable[Sequence[Any]]) -> CounterType[tuple[Any, ...]]:
    return Counter(tuple(row) for row in rows)


def _normalize_rows(rows: Any) -> Optional[CounterType[tuple[Any, ...]]]:
    if rows is None:
        return None
    if isinstance(rows, dict):  # error payload
        return None
    if not isinstance(rows, list):
        raise TypeError(f"Unexpected rows payload type: {type(rows)!r}")
    return _rows_to_counter(rows)


def evaluate_entry(entry: dict[str, Any], max_diff_rows: int) -> EntryEvaluation:
    identifier = entry.get("id", "<unknown>")
    gt_rows_raw = entry.get("ground_truth_result")
    gen_rows_raw = entry.get("generated_result")

    gt_counter = _normalize_rows(gt_rows_raw) or Counter()

    if isinstance(gen_rows_raw, dict):
        error_msg = gen_rows_raw.get("error", "Unknown error")
        return EntryEvaluation(
            identifier=identifier,
            match=False,
            status="error",
            ground_truth_rows=sum(gt_counter.values()),
            generated_rows=None,
            error=error_msg,
        )

    if gen_rows_raw is None:
        return EntryEvaluation(
            identifier=identifier,
            match=False,
            status="missing",
            ground_truth_rows=sum(gt_counter.values()),
            generated_rows=None,
        )

    gen_counter = _normalize_rows(gen_rows_raw) or Counter()
    match = gt_counter == gen_counter

    missing_rows = extra_rows = None
    if not match:
        missing = list((gt_counter - gen_counter).elements())
        extra = list((gen_counter - gt_counter).elements())
        if missing:
            missing_rows = [list(row) for row in missing[:max_diff_rows]]
        if extra:
            extra_rows = [list(row) for row in extra[:max_diff_rows]]

    return EntryEvaluation(
        identifier=identifier,
        match=match,
        status="match" if match else "mismatch",
        ground_truth_rows=sum(gt_counter.values()),
        generated_rows=sum(gen_counter.values()),
        missing_rows=missing_rows,
        extra_rows=extra_rows,
    )


def report_evaluation(entries: Sequence[EntryEvaluation]) -> None:
    print("=== Agent SQL Evaluation ===\n")

    for item in entries:
        print(f"{item.identifier}: {item.status.upper()}")
        if item.match:
            print(f"  ✅ Results match ({item.ground_truth_rows} rows)")
        elif item.status == "error":
            print("  ❌ Execution error")
            if item.error:
                print(f"  Reason: {item.error}")
        elif item.status == "missing":
            print("  ❌ No generated result available")
        else:
            print(
                "  ❌ Results differ",
                f"(GT rows: {item.ground_truth_rows}, Generated rows: {item.generated_rows})",
            )
            if item.missing_rows:
                print(f"  Missing rows (sample {len(item.missing_rows)}): {item.missing_rows}")
            if item.extra_rows:
                print(f"  Extra rows (sample {len(item.extra_rows)}): {item.extra_rows}")
        print()

    total = len(entries)
    matches = sum(1 for item in entries if item.match)
    errors = sum(1 for item in entries if item.status == "error")
    missing = sum(1 for item in entries if item.status == "missing")
    mismatches = sum(1 for item in entries if item.status == "mismatch")

    print("=== Summary ===")
    print(f"Total cases: {total}")
    print(f"Matches: {matches}")
    print(f"Mismatches: {mismatches}")
    print(f"Execution errors: {errors}")
    print(f"Missing results: {missing}")
    if total:
        accuracy = matches / total * 100
        print(f"Match rate: {accuracy:.1f}%")


def main() -> None:
    args = parse_args()
    results = _load_results(args.input)
    evaluations = [evaluate_entry(entry, args.max_diff_rows) for entry in results]
    report_evaluation(evaluations)


if __name__ == "__main__":
    main()

# python evaluation/scripts/agent_eval_sql_results.py --input evaluation/results/tables_result_15_outputs_outputs_2.json