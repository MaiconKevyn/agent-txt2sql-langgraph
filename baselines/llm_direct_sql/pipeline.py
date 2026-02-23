from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from evaluation.metrics.base_metrics import EvaluationContext
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric

from baselines.llm_direct_sql.config import BaselineConfig
from baselines.llm_direct_sql.context_loader import build_schema_context
from baselines.llm_direct_sql.llm_client import DirectLLMClient
from baselines.llm_direct_sql.prompt_builder import build_prompts
from baselines.llm_direct_sql.query_executor import PostgresQueryExecutor
from baselines.llm_direct_sql.sql_parser import parse_and_validate_sql


def load_ground_truth(path: str) -> List[Dict[str, Any]]:
    gt_path = Path(path)
    if not gt_path.exists():
        raise FileNotFoundError(f"Ground truth file not found: {gt_path}")
    with gt_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Ground truth file must contain a list of questions")
    return data


def default_output_path(output_dir: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return output_dir / f"llm_direct_baseline_{ts}.json"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def run_single_question(
    question: str,
    config: BaselineConfig,
    *,
    question_id: str = "single",
    difficulty: str = "ad-hoc",
    ground_truth_sql: str = "",
    schema_context: Optional[str] = None,
) -> Dict[str, Any]:
    context = schema_context or build_schema_context()
    system_prompt, user_prompt = build_prompts(question, context)

    llm = DirectLLMClient(config)
    db = PostgresQueryExecutor(
        database_url=config.database_url,
        statement_timeout_ms=config.statement_timeout_ms,
    )
    try:
        llm_output = llm.generate_sql(system_prompt, user_prompt)
        parsed = parse_and_validate_sql(llm_output.raw_text)

        predicted_sql = parsed.sql if parsed.is_safe else ""
        execution = None
        if predicted_sql:
            execution = db.execute(predicted_sql)

        return {
            "question_id": question_id,
            "difficulty": difficulty,
            "question": question,
            "ground_truth_sql": ground_truth_sql,
            "predicted_sql": predicted_sql,
            "unsafe_predicted_sql": parsed.sql if not parsed.is_safe else "",
            "is_safe_sql": parsed.is_safe,
            "safety_reason": parsed.safety_reason,
            "llm_provider": config.llm_provider,
            "llm_model": config.llm_model,
            "llm_latency_s": llm_output.latency_s,
            "raw_response": llm_output.raw_text if config.include_raw_response else "",
            "execution": {
                "row_count": execution.row_count if execution else 0,
                "columns": execution.columns if execution else [],
                "rows_preview": list(execution.rows[:5]) if execution else [],
                "error": execution.error if execution else (
                    parsed.safety_reason if not parsed.is_safe else None
                ),
                "latency_s": execution.latency_s if execution else 0.0,
            },
            "agent_success": bool(predicted_sql.strip()),
        }
    finally:
        db.close()


def _aggregate_results(
    results: List[Dict[str, Any]],
    metric_scores: Dict[str, List[float]],
) -> Dict[str, Any]:
    total = len(results)
    success_count = sum(1 for r in results if r.get("agent_success"))
    unsafe_count = sum(1 for r in results if not r.get("is_safe_sql", False))
    exec_error_count = sum(
        1
        for r in results
        if r.get("agent_success") and r.get("execution", {}).get("error")
    )
    total_llm_time = sum(float(r.get("llm_latency_s", 0.0)) for r in results)
    total_exec_time = sum(float(r.get("execution", {}).get("latency_s", 0.0)) for r in results)

    aggregated_metrics: Dict[str, Dict[str, Any]] = {}
    for metric_name, scores in metric_scores.items():
        if scores:
            aggregated_metrics[metric_name] = {
                "average_score": sum(scores) / len(scores),
                "accuracy": sum(1 for s in scores if s >= 0.8) / len(scores),
                "perfect_matches": sum(1 for s in scores if s == 1.0),
                "total_evaluated": len(scores),
            }
        else:
            aggregated_metrics[metric_name] = {
                "average_score": 0.0,
                "accuracy": 0.0,
                "perfect_matches": 0,
                "total_evaluated": 0,
            }

    by_difficulty: Dict[str, Dict[str, Any]] = {}
    for row in results:
        diff = row.get("difficulty", "unknown")
        section = by_difficulty.setdefault(
            diff,
            {
                "total": 0,
                "agent_success": 0,
                "safe_sql": 0,
                "metrics": {},
            },
        )
        section["total"] += 1
        if row.get("agent_success"):
            section["agent_success"] += 1
        if row.get("is_safe_sql"):
            section["safe_sql"] += 1

        if row.get("agent_success"):
            for metric_name, metric in row.get("metrics", {}).items():
                metric_bucket = section["metrics"].setdefault(
                    metric_name, {"correct": 0, "total": 0, "scores": []}
                )
                metric_bucket["total"] += 1
                metric_bucket["scores"].append(metric.get("score", 0.0))
                if metric.get("is_correct"):
                    metric_bucket["correct"] += 1

    return {
        "summary": {
            "total_questions": total,
            "agent_success_rate": (success_count / total) if total else 0.0,
            "unsafe_sql_rate": (unsafe_count / total) if total else 0.0,
            "execution_error_rate": (exec_error_count / total) if total else 0.0,
            "avg_llm_latency_s": (total_llm_time / total) if total else 0.0,
            "avg_execution_latency_s": (total_exec_time / total) if total else 0.0,
            "total_llm_latency_s": total_llm_time,
            "total_execution_latency_s": total_exec_time,
        },
        "metrics": aggregated_metrics,
        "difficulty_breakdown": by_difficulty,
    }


def run_batch(
    questions: List[Dict[str, Any]],
    config: BaselineConfig,
    *,
    include_metrics: bool = True,
    schema_context: Optional[str] = None,
) -> Dict[str, Any]:
    context = schema_context or build_schema_context()
    llm = DirectLLMClient(config)
    db = PostgresQueryExecutor(
        database_url=config.database_url,
        statement_timeout_ms=config.statement_timeout_ms,
    )

    metrics = []
    if include_metrics:
        metrics = [
            ExactMatchMetric(),
            ComponentMatchingMetric(),
            ExecutionAccuracyMetric(execution_timeout=max(1, config.llm_timeout)),
        ]

    detailed_results: List[Dict[str, Any]] = []
    metric_scores: Dict[str, List[float]] = {metric.name: [] for metric in metrics}

    try:
        for item in questions:
            question = item.get("question", "")
            question_id = item.get("id", "")
            difficulty = item.get("difficulty", "unknown")
            ground_truth_sql = item.get("query", "")

            system_prompt, user_prompt = build_prompts(question, context)
            llm_output = llm.generate_sql(system_prompt, user_prompt)
            parsed = parse_and_validate_sql(llm_output.raw_text)

            predicted_sql = parsed.sql if parsed.is_safe else ""
            execution = None
            if predicted_sql:
                execution = db.execute(predicted_sql)

            row_result: Dict[str, Any] = {
                "question_id": question_id,
                "difficulty": difficulty,
                "question": question,
                "ground_truth_sql": ground_truth_sql,
                "predicted_sql": predicted_sql,
                "unsafe_predicted_sql": parsed.sql if not parsed.is_safe else "",
                "is_safe_sql": parsed.is_safe,
                "safety_reason": parsed.safety_reason,
                "agent_success": bool(predicted_sql.strip()),
                "llm_latency_s": llm_output.latency_s,
                "raw_response": llm_output.raw_text if config.include_raw_response else "",
                "execution": {
                    "row_count": execution.row_count if execution else 0,
                    "error": execution.error if execution else (
                        parsed.safety_reason if not parsed.is_safe else None
                    ),
                    "latency_s": execution.latency_s if execution else 0.0,
                },
                "metrics": {},
            }

            if include_metrics:
                eval_context = EvaluationContext(
                    question_id=question_id,
                    question=question,
                    ground_truth_sql=ground_truth_sql,
                    predicted_sql=predicted_sql,
                    database_connection=db,
                )
                for metric in metrics:
                    try:
                        metric_result = metric.evaluate(eval_context)
                        row_result["metrics"][metric.name] = {
                            "score": metric_result.score,
                            "is_correct": metric_result.is_correct,
                            "details": metric_result.details,
                            "error": metric_result.error_message,
                        }
                        if predicted_sql.strip():
                            metric_scores[metric.name].append(metric_result.score)
                    except Exception as exc:
                        row_result["metrics"][metric.name] = {
                            "score": 0.0,
                            "is_correct": False,
                            "details": {},
                            "error": str(exc),
                        }

            detailed_results.append(row_result)

    finally:
        db.close()

    aggregate = _aggregate_results(detailed_results, metric_scores)
    return {
        "baseline": {
            "name": "llm_direct_sql",
            "description": "Single-shot direct LLM baseline without LangGraph",
            "provider": config.llm_provider,
            "model": config.llm_model,
            "temperature": config.llm_temperature,
            "statement_timeout_ms": config.statement_timeout_ms,
        },
        "timestamp": datetime.now().isoformat(),
        "summary": aggregate["summary"],
        "metrics": aggregate["metrics"],
        "difficulty_breakdown": aggregate["difficulty_breakdown"],
        "total_questions": len(detailed_results),
        "detailed_results": detailed_results,
    }


def save_results(results: Dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(_json_safe(results), f, indent=2, ensure_ascii=False)
    return output_path

