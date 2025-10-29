"""
Task Functions for Evaluation Pipeline DAG

This module contains all task functions used in the evaluation pipeline.
Each task is a standalone function that takes inputs from previous tasks
and returns data for downstream tasks.
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext
from src.agent.orchestrator import LangGraphOrchestrator
from src.application.config.simple_config import ApplicationConfig


# ============================================================================
# Configuration and Initialization Tasks
# ============================================================================

def load_configuration(**kwargs) -> Dict[str, Any]:
    """
    Load application configuration

    Returns:
        Dict containing configuration objects
    """
    print("  Loading application configuration...")

    config = ApplicationConfig()

    return {
        'config': config,
        'llm_provider': config.llm_provider,
        'llm_model': config.llm_model
    }


def load_ground_truth(**kwargs) -> Dict[str, Any]:
    """
    Load ground truth questions from JSON file

    Returns:
        Dict containing questions list and metadata
    """
    print("  Loading ground truth data...")

    gt_path = project_root / "evaluation" / "ground_truth.json"

    if not gt_path.exists():
        raise FileNotFoundError(f"Ground truth file not found: {gt_path}")

    with open(gt_path, 'r', encoding='utf-8') as f:
        questions = json.load(f)

    # Calculate statistics
    difficulty_counts = {}
    for q in questions:
        diff = q.get('difficulty', 'unknown')
        difficulty_counts[diff] = difficulty_counts.get(diff, 0) + 1

    print(f"    Loaded {len(questions)} questions")
    print(f"    Difficulty breakdown: {difficulty_counts}")

    return {
        'questions': questions,
        'total_count': len(questions),
        'difficulty_breakdown': difficulty_counts
    }


def initialize_database(load_configuration: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Initialize database connection

    Args:
        load_configuration: Configuration from load_configuration task

    Returns:
        Dict containing database connection wrapper
    """
    print("  Initializing database connection...")

    # Simple database wrapper
    class SimpleDatabaseConnection:
        def __init__(self, db_url: str):
            import psycopg2
            self.db_url = db_url
            self.connection = psycopg2.connect(db_url)

        def execute_query(self, sql: str):
            try:
                cursor = self.connection.cursor()
                cursor.execute(sql)
                try:
                    results = cursor.fetchall()
                    return results, None
                except Exception:
                    return [], None
            except Exception as e:
                return None, str(e)

        def get_raw_connection(self):
            return self.connection

        def close(self):
            if self.connection:
                self.connection.close()

    # Get database URL
    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH")

    if not db_url:
        raise ValueError("DATABASE_URL or DATABASE_PATH not found in environment")

    # Convert SQLAlchemy format to psycopg2 format if needed
    if "postgresql+psycopg2://" in db_url:
        db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")

    db_connection = SimpleDatabaseConnection(db_url)

    print("    Database connected successfully")

    return {
        'db_connection': db_connection,
        'db_url_masked': db_url.split('@')[1] if '@' in db_url else 'localhost'
    }


def initialize_metrics(load_configuration: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Initialize evaluation metrics

    Args:
        load_configuration: Configuration from load_configuration task

    Returns:
        Dict containing metric instances
    """
    print("  Initializing evaluation metrics...")

    metrics = [
        ExactMatchMetric(),
        ComponentMatchingMetric(),
        ExecutionAccuracyMetric(execution_timeout=60)
    ]

    print(f"    Initialized {len(metrics)} metrics:")
    for metric in metrics:
        print(f"      - {metric.name}")

    return {
        'metrics': metrics,
        'metric_names': [m.name for m in metrics]
    }


def initialize_agent(load_configuration: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Initialize LangGraph agent orchestrator

    Args:
        load_configuration: Configuration from load_configuration task

    Returns:
        Dict containing agent instance
    """
    print("  Initializing LangGraph agent...")

    app_config = load_configuration['config']
    agent = LangGraphOrchestrator(app_config)

    print(f"    Agent initialized:")
    print(f"      Provider: {app_config.llm_provider}")
    print(f"      Model: {app_config.llm_model}")

    return {
        'agent': agent,
        'agent_config': {
            'provider': app_config.llm_provider,
            'model': app_config.llm_model
        }
    }


# ============================================================================
# Evaluation Execution Tasks
# ============================================================================

def evaluate_questions(
    load_ground_truth: Dict[str, Any],
    initialize_metrics: Dict[str, Any],
    initialize_agent: Dict[str, Any],
    initialize_database: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Evaluate all questions using the agent and metrics

    Supports parallel execution to speed up evaluation.

    Args:
        load_ground_truth: Ground truth data
        initialize_metrics: Metric instances
        initialize_agent: Agent instance
        initialize_database: Database connection
        **kwargs: Can include 'max_workers' (default=1)
                  1=sequential, 2+=parallel
                  Recommended: 1-2 for GPU, 2-4 for CPU-only

    Returns:
        Dict containing detailed evaluation results
    """
    questions = load_ground_truth['questions']
    metrics = initialize_metrics['metrics']
    agent = initialize_agent['agent']
    db_connection = initialize_database['db_connection']

    # Get max_workers from kwargs (default to 1 for sequential)
    max_workers = kwargs.get('max_workers', 1)

    total = len(questions)

    # Determine execution mode
    if max_workers > 1:
        print(f"  Evaluating {total} questions with {max_workers} parallel workers...")
        print(f"    ⚠️  Using parallel mode - monitor GPU memory!")
        return _evaluate_questions_parallel(
            questions, metrics, agent, db_connection, max_workers
        )
    else:
        print(f"  Evaluating {total} questions sequentially...")
        return _evaluate_questions_sequential(
            questions, metrics, agent, db_connection
        )


def _evaluate_questions_sequential(
    questions: List[Dict],
    metrics: List,
    agent,
    db_connection
) -> Dict[str, Any]:
    """Sequential evaluation (original implementation)"""
    results = []
    metric_scores = {metric.name: [] for metric in metrics}

    # Agent statistics
    agent_stats = {
        'success_count': 0,
        'failure_count': 0,
        'total_time': 0.0
    }

    total = len(questions)

    for i, question_data in enumerate(questions, 1):
        if i % 10 == 0:
            print(f"      Progress: {i}/{total} ({i/total*100:.1f}%)")

        # Generate prediction with agent
        start_time = time.time()

        try:
            agent_result = agent.process_query(question_data['question'])

            # Extract SQL
            if isinstance(agent_result, dict):
                predicted_sql = agent_result.get('sql_query', '')
                agent_success = agent_result.get('success', False)
            else:
                predicted_sql = str(agent_result)
                agent_success = bool(predicted_sql.strip())

            execution_time = time.time() - start_time
            agent_stats['total_time'] += execution_time

            if agent_success and predicted_sql.strip():
                agent_stats['success_count'] += 1
            else:
                agent_stats['failure_count'] += 1
                predicted_sql = ""

        except Exception as e:
            execution_time = time.time() - start_time
            agent_stats['failure_count'] += 1
            agent_stats['total_time'] += execution_time
            predicted_sql = ""

        # Evaluate with metrics
        context = EvaluationContext(
            question_id=question_data['id'],
            question=question_data['question'],
            ground_truth_sql=question_data['query'],
            predicted_sql=predicted_sql,
            database_connection=db_connection
        )

        question_results = {
            'question_id': question_data['id'],
            'difficulty': question_data['difficulty'],
            'question': question_data['question'],
            'ground_truth_sql': question_data['query'],
            'predicted_sql': predicted_sql,
            'agent_success': bool(predicted_sql.strip()),
            'agent_execution_time': execution_time,
            'metrics': {}
        }

        for metric in metrics:
            try:
                result = metric.evaluate(context)
                question_results['metrics'][metric.name] = {
                    'score': result.score,
                    'is_correct': result.is_correct,
                    'error': result.error_message,
                    'details': result.details
                }

                if predicted_sql.strip():
                    metric_scores[metric.name].append(result.score)

            except Exception as e:
                question_results['metrics'][metric.name] = {
                    'score': 0.0,
                    'is_correct': False,
                    'error': str(e)
                }

        results.append(question_results)

    print(f"    Evaluation completed:")
    print(f"      Agent success: {agent_stats['success_count']}/{total} ({agent_stats['success_count']/total*100:.1f}%)")
    print(f"      Total time: {agent_stats['total_time']:.1f}s")

    return {
        'detailed_results': results,
        'agent_stats': agent_stats,
        'metric_scores': metric_scores,
        'total_questions': total
    }


def _evaluate_questions_parallel(
    questions: List[Dict],
    metrics: List,
    agent,
    db_connection,
    max_workers: int
) -> Dict[str, Any]:
    """
    Parallel evaluation using ThreadPoolExecutor

    Args:
        questions: List of question dictionaries
        metrics: List of metric instances
        agent: Agent orchestrator
        db_connection: Database connection
        max_workers: Number of parallel workers

    Returns:
        Dict containing detailed evaluation results
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    results = []
    metric_scores = {metric.name: [] for metric in metrics}

    # Agent statistics (thread-safe)
    agent_stats = {
        'success_count': 0,
        'failure_count': 0,
        'total_time': 0.0
    }
    stats_lock = threading.Lock()
    results_lock = threading.Lock()

    total = len(questions)
    completed_count = 0
    count_lock = threading.Lock()

    def evaluate_single_question(question_data):
        """Evaluate a single question (runs in thread)"""
        nonlocal completed_count

        start_time = time.time()

        try:
            agent_result = agent.process_query(question_data['question'])

            # Extract SQL
            if isinstance(agent_result, dict):
                predicted_sql = agent_result.get('sql_query', '')
                agent_success = agent_result.get('success', False)
            else:
                predicted_sql = str(agent_result)
                agent_success = bool(predicted_sql.strip())

            execution_time = time.time() - start_time

            # Update stats (thread-safe)
            with stats_lock:
                agent_stats['total_time'] += execution_time
                if agent_success and predicted_sql.strip():
                    agent_stats['success_count'] += 1
                else:
                    agent_stats['failure_count'] += 1
                    predicted_sql = ""

        except Exception as e:
            execution_time = time.time() - start_time
            with stats_lock:
                agent_stats['failure_count'] += 1
                agent_stats['total_time'] += execution_time
            predicted_sql = ""

        # Evaluate with metrics
        context = EvaluationContext(
            question_id=question_data['id'],
            question=question_data['question'],
            ground_truth_sql=question_data['query'],
            predicted_sql=predicted_sql,
            database_connection=db_connection
        )

        question_results = {
            'question_id': question_data['id'],
            'difficulty': question_data['difficulty'],
            'question': question_data['question'],
            'ground_truth_sql': question_data['query'],
            'predicted_sql': predicted_sql,
            'agent_success': bool(predicted_sql.strip()),
            'agent_execution_time': execution_time,
            'metrics': {}
        }

        for metric in metrics:
            try:
                result = metric.evaluate(context)
                question_results['metrics'][metric.name] = {
                    'score': result.score,
                    'is_correct': result.is_correct,
                    'error': result.error_message,
                    'details': result.details
                }

                if predicted_sql.strip():
                    with results_lock:
                        metric_scores[metric.name].append(result.score)

            except Exception as e:
                question_results['metrics'][metric.name] = {
                    'score': 0.0,
                    'is_correct': False,
                    'error': str(e)
                }

        # Update progress (thread-safe)
        with count_lock:
            completed_count += 1
            if completed_count % 10 == 0:
                print(f"      Progress: {completed_count}/{total} ({completed_count/total*100:.1f}%)")

        return question_results

    # Execute in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(evaluate_single_question, q): q for q in questions}

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                with results_lock:
                    results.append(result)
            except Exception as e:
                question = futures[future]
                print(f"      Error processing {question['id']}: {e}")

    print(f"    Evaluation completed:")
    print(f"      Agent success: {agent_stats['success_count']}/{total} ({agent_stats['success_count']/total*100:.1f}%)")
    print(f"      Total time: {agent_stats['total_time']:.1f}s")
    print(f"      Speedup: {max_workers}x workers")

    return {
        'detailed_results': results,
        'agent_stats': agent_stats,
        'metric_scores': metric_scores,
        'total_questions': total
    }


def aggregate_results(evaluate_questions: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Aggregate evaluation results and calculate statistics

    Args:
        evaluate_questions: Results from evaluation

    Returns:
        Dict containing aggregated statistics
    """
    print("  Aggregating results...")

    results = evaluate_questions['detailed_results']
    agent_stats = evaluate_questions['agent_stats']
    metric_scores = evaluate_questions['metric_scores']
    total = evaluate_questions['total_questions']

    # Calculate metric statistics
    aggregated_metrics = {}

    for metric_name, scores in metric_scores.items():
        if scores:
            avg_score = sum(scores) / len(scores)
            accuracy = sum(1 for s in scores if s >= 0.8) / len(scores)
            perfect = sum(1 for s in scores if s == 1.0)

            aggregated_metrics[metric_name] = {
                'average_score': avg_score,
                'accuracy': accuracy,
                'perfect_matches': perfect,
                'total_evaluated': len(scores)
            }
        else:
            aggregated_metrics[metric_name] = {
                'average_score': 0.0,
                'accuracy': 0.0,
                'perfect_matches': 0,
                'total_evaluated': 0
            }

    # Difficulty breakdown
    difficulties = {}
    for r in results:
        diff = r['difficulty']
        if diff not in difficulties:
            difficulties[diff] = {
                'total': 0,
                'agent_success': 0,
                'metrics': {}
            }

        difficulties[diff]['total'] += 1

        if r['agent_success']:
            difficulties[diff]['agent_success'] += 1

            # Collect metric stats per difficulty
            for metric_name, metric_result in r['metrics'].items():
                if metric_name not in difficulties[diff]['metrics']:
                    difficulties[diff]['metrics'][metric_name] = {
                        'correct': 0,
                        'total': 0,
                        'scores': []
                    }

                difficulties[diff]['metrics'][metric_name]['total'] += 1
                difficulties[diff]['metrics'][metric_name]['scores'].append(
                    metric_result['score']
                )

                if metric_result['is_correct']:
                    difficulties[diff]['metrics'][metric_name]['correct'] += 1

    print(f"    Aggregated {len(results)} results")
    print(f"    Metrics:")
    for metric_name, stats in aggregated_metrics.items():
        print(f"      {metric_name}: {stats['average_score']:.3f} avg, {stats['accuracy']:.1%} accuracy")

    return {
        'summary': {
            'total_questions': total,
            'agent_success_rate': agent_stats['success_count'] / total if total > 0 else 0,
            'agent_failure_rate': agent_stats['failure_count'] / total if total > 0 else 0,
            'total_execution_time': agent_stats['total_time'],
            'avg_execution_time': agent_stats['total_time'] / total if total > 0 else 0
        },
        'metrics': aggregated_metrics,
        'difficulty_breakdown': difficulties,
        'timestamp': datetime.now().isoformat()
    }


def generate_report(
    aggregate_results: Dict[str, Any],
    evaluate_questions: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Generate human-readable report

    Args:
        aggregate_results: Aggregated statistics
        evaluate_questions: Detailed results

    Returns:
        Dict containing report text
    """
    print("  Generating evaluation report...")

    summary = aggregate_results['summary']
    metrics = aggregate_results['metrics']
    difficulties = aggregate_results['difficulty_breakdown']

    report_lines = []
    report_lines.append("="*80)
    report_lines.append("TEXT-TO-SQL EVALUATION REPORT")
    report_lines.append("="*80)
    report_lines.append("")

    # Overall summary
    report_lines.append("OVERALL SUMMARY")
    report_lines.append("-"*80)
    report_lines.append(f"Total Questions: {summary['total_questions']}")
    report_lines.append(f"Agent Success Rate: {summary['agent_success_rate']:.1%}")
    report_lines.append(f"Total Execution Time: {summary['total_execution_time']:.1f}s")
    report_lines.append(f"Avg Time per Question: {summary['avg_execution_time']:.2f}s")
    report_lines.append("")

    # Metrics
    report_lines.append("METRICS PERFORMANCE")
    report_lines.append("-"*80)
    for metric_name, stats in metrics.items():
        report_lines.append(f"\n{metric_name}:")
        report_lines.append(f"  Average Score: {stats['average_score']:.3f}")
        report_lines.append(f"  Accuracy (≥0.8): {stats['accuracy']:.1%}")
        report_lines.append(f"  Perfect Matches: {stats['perfect_matches']}/{stats['total_evaluated']}")

    report_lines.append("")

    # Difficulty breakdown
    report_lines.append("DIFFICULTY BREAKDOWN")
    report_lines.append("-"*80)
    for diff, stats in sorted(difficulties.items()):
        report_lines.append(f"\n{diff.upper()}:")
        report_lines.append(f"  Questions: {stats['total']}")
        report_lines.append(f"  Agent Success: {stats['agent_success']}/{stats['total']} "
                          f"({stats['agent_success']/stats['total']*100:.1f}%)")

        for metric_name, metric_stats in stats['metrics'].items():
            if metric_stats['total'] > 0:
                avg_score = sum(metric_stats['scores']) / len(metric_stats['scores'])
                accuracy = metric_stats['correct'] / metric_stats['total']
                report_lines.append(f"  {metric_name}: {avg_score:.3f} ({accuracy:.1%})")

    report_lines.append("")
    report_lines.append("="*80)

    report_text = "\n".join(report_lines)

    print("    Report generated successfully")

    return {
        'report_text': report_text,
        'report_lines': report_lines
    }


def _generate_execution_outputs_file(
    detailed_results: List[Dict],
    output_path: Path,
    db_connection
) -> None:
    """
    Generate a text file showing execution outputs for manual validation

    For each ground truth question, shows:
    - Question ID and text
    - Ground truth SQL and execution results
    - Predicted SQL and execution results
    - Execution Accuracy (EX) metric

    Args:
        detailed_results: List of evaluation results from evaluate_questions
        output_path: Path where to save the outputs file
        db_connection: Database connection to execute queries
    """
    lines = []
    lines.append("="*80)
    lines.append("QUERY EXECUTION OUTPUTS - MANUAL VALIDATION")
    lines.append("="*80)
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Total Questions: {len(detailed_results)}")
    lines.append("")

    for i, result in enumerate(detailed_results, 1):
        lines.append("="*80)
        lines.append(f"{result['question_id']}: {result['question']}")
        lines.append(f"Difficulty: {result['difficulty'].upper()}")
        lines.append("="*80)
        lines.append("")

        # Ground Truth SQL
        lines.append("Ground Truth SQL:")
        lines.append("  " + result['ground_truth_sql'])
        lines.append("")

        # Execute ground truth query
        gt_results, gt_error = db_connection.execute_query(result['ground_truth_sql'])

        lines.append("Ground Truth Results:")
        if gt_error:
            lines.append(f"  ERROR: {gt_error}")
        elif gt_results is not None:
            if len(gt_results) == 0:
                lines.append("  [Empty result set]")
            else:
                for idx, row in enumerate(gt_results[:10], 1):  # Show first 10 rows
                    lines.append(f"  Row {idx}: {list(row)}")
                if len(gt_results) > 10:
                    lines.append(f"  ... ({len(gt_results) - 10} more rows)")
                lines.append(f"  Total rows: {len(gt_results)}")
        else:
            lines.append("  [No results available]")
        lines.append("")

        # Predicted SQL
        if result['agent_success']:
            lines.append("Predicted SQL:")
            lines.append("  " + result['predicted_sql'])
            lines.append("")

            # Execute predicted query
            pred_results, pred_error = db_connection.execute_query(result['predicted_sql'])

            lines.append("Predicted Results:")
            if pred_error:
                lines.append(f"  ERROR: {pred_error}")
            elif pred_results is not None:
                if len(pred_results) == 0:
                    lines.append("  [Empty result set]")
                else:
                    for idx, row in enumerate(pred_results[:10], 1):  # Show first 10 rows
                        lines.append(f"  Row {idx}: {list(row)}")
                    if len(pred_results) > 10:
                        lines.append(f"  ... ({len(pred_results) - 10} more rows)")
                    lines.append(f"  Total rows: {len(pred_results)}")
            else:
                lines.append("  [No results available]")
        else:
            lines.append("Predicted SQL:")
            lines.append("  [AGENT FAILED - No SQL generated]")
            lines.append("")
            lines.append("Predicted Results:")
            lines.append("  [Agent failed - no SQL to execute]")
        lines.append("")

        # EX Metric
        ex_metric = result['metrics'].get('execution_accuracy', {})

        if ex_metric:
            score = ex_metric.get('score', 0.0)
            is_correct = ex_metric.get('is_correct', False)
            error = ex_metric.get('error')

            lines.append(f"Execution Accuracy (EX): {score:.1f}")
            if is_correct:
                lines.append("Status: ✓ CORRECT (Results match)")
            else:
                lines.append("Status: ✗ INCORRECT (Results differ)")

            if error:
                lines.append(f"Error: {error}")
        else:
            lines.append("Execution Accuracy: [Not available]")

        lines.append("")

        # Add separator between questions
        if i < len(detailed_results):
            lines.append("")

    lines.append("="*80)
    lines.append("END OF REPORT")
    lines.append("="*80)

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))


def save_results(
    evaluate_questions: Dict[str, Any],
    aggregate_results: Dict[str, Any],
    generate_report: Dict[str, Any],
    load_configuration: Dict[str, Any],
    initialize_agent: Dict[str, Any],
    initialize_database: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Save results to JSON file and print report

    Args:
        evaluate_questions: Detailed results
        aggregate_results: Aggregated statistics
        generate_report: Report text
        load_configuration: Configuration data
        initialize_agent: Agent configuration
        initialize_database: Database connection

    Returns:
        Dict containing output paths
    """
    print("  Saving results...")

    # Prepare complete results
    complete_results = {
        'evaluation_timestamp': aggregate_results['timestamp'],
        'configuration': {
            'llm_provider': load_configuration['llm_provider'],
            'llm_model': load_configuration['llm_model']
        },
        'agent_config': initialize_agent['agent_config'],
        'summary': aggregate_results['summary'],
        'metrics': aggregate_results['metrics'],
        'difficulty_breakdown': aggregate_results['difficulty_breakdown'],
        'detailed_results': evaluate_questions['detailed_results']
    }

    # Save JSON results
    output_dir = project_root / "evaluation" / "results"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"dag_evaluation_{timestamp}.json"

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(complete_results, f, indent=2, ensure_ascii=False)

    # Save report text
    report_path = output_dir / f"dag_evaluation_report_{timestamp}.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(generate_report['report_text'])

    # Generate execution outputs file for manual validation
    outputs_path = output_dir / f"dag_execution_outputs_{timestamp}.txt"
    _generate_execution_outputs_file(
        detailed_results=evaluate_questions['detailed_results'],
        output_path=outputs_path,
        db_connection=initialize_database['db_connection']
    )

    print(f"    Results saved:")
    print(f"      JSON: {json_path}")
    print(f"      Report: {report_path}")
    print(f"      Execution Outputs: {outputs_path}")

    # Print report to console
    print("\n")
    print(generate_report['report_text'])

    return {
        'json_path': str(json_path),
        'report_path': str(report_path),
        'outputs_path': str(outputs_path),
        'saved_successfully': True
    }


# Cleanup task
def cleanup_resources(initialize_database: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Cleanup resources (close database connections, etc.)

    Args:
        initialize_database: Database connection to close

    Returns:
        Dict with cleanup status
    """
    print("  Cleaning up resources...")

    db_connection = initialize_database['db_connection']

    try:
        db_connection.close()
        print("    Database connection closed")
        return {'cleanup_successful': True}
    except Exception as e:
        print(f"    Warning: Cleanup error - {e}")
        return {'cleanup_successful': False, 'error': str(e)}
