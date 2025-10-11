#!/usr/bin/env python3
"""
Full Evaluation with Real Agent and All Metrics (EM, CM, EX)

This script runs complete evaluation on ALL ground truth questions using:
- Real LangGraph agent (not mock)
- All 3 metrics: Exact Match, Component Matching, Execution Accuracy
- Real PostgreSQL database connection
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext

# Import real agent
from src.agent.orchestrator import LangGraphOrchestrator
from src.application.config.simple_config import ApplicationConfig


class SimpleDatabaseConnection:
    """Simple database connection wrapper for PostgreSQL"""

    def __init__(self, db_url: str):
        import psycopg2
        self.db_url = db_url
        self.connection = psycopg2.connect(db_url)

    def execute_query(self, sql: str):
        """Execute query and return results"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            try:
                results = cursor.fetchall()
                return results, None
            except Exception:
                # No results to fetch (e.g., DDL statements)
                return [], None

        except Exception as e:
            return None, str(e)

    def get_raw_connection(self):
        """Get raw psycopg2 connection"""
        return self.connection

    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()


def load_ground_truth(file_path: Path) -> list:
    """Load all questions from ground truth"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def run_full_evaluation():
    """Run complete evaluation with real agent on all questions"""
    print("="*80)
    print("FULL EVALUATION - ALL GROUND TRUTH QUESTIONS WITH REAL AGENT")
    print("="*80)

    # Check for database URL
    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH")
    if not db_url:
        print("❌ ERROR: DATABASE_URL or DATABASE_PATH not found in environment")
        print("   Please set DATABASE_URL or DATABASE_PATH in .env file")
        return None

    # Convert SQLAlchemy format to psycopg2 format if needed
    if "postgresql+psycopg2://" in db_url:
        db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")

    print(f"✅ Database URL found")

    # Connect to database
    try:
        db_connection = SimpleDatabaseConnection(db_url)
        print(f"✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None

    # Load ALL ground truth questions
    gt_path = project_root / "evaluation" / "ground_truth.json"
    if not gt_path.exists():
        print(f"❌ Ground truth file not found: {gt_path}")
        return None

    all_questions = load_ground_truth(gt_path)
    total_questions = len(all_questions)
    print(f"✅ Loaded {total_questions} questions from ground truth")

    # Initialize real agent
    print(f"\n{'='*80}")
    print("INITIALIZING REAL AGENT")
    print("="*80)
    try:
        config = ApplicationConfig()
        agent = LangGraphOrchestrator(config)
        print(f"✅ LangGraph agent initialized")
        print(f"   Provider: {config.llm_provider}")
        print(f"   Model: {config.llm_model}")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {e}")
        return None

    # Initialize metrics (ALL THREE including EX)
    metrics = [
        ExactMatchMetric(),
        ComponentMatchingMetric(),
        ExecutionAccuracyMetric(execution_timeout=60)
    ]
    print(f"✅ Initialized {len(metrics)} metrics (EM, CM, EX)")

    # Results storage
    results = []
    metric_scores = {metric.name: [] for metric in metrics}

    # Track agent performance
    agent_stats = {
        'success_count': 0,
        'failure_count': 0,
        'total_time': 0.0
    }

    print(f"\n{'='*80}")
    print(f"EVALUATING ALL {total_questions} QUESTIONS")
    print("="*80)

    # Evaluate each question
    for i, question_data in enumerate(all_questions, 1):
        print(f"\n[{i:3d}/{total_questions}] {question_data['id']} - {question_data['difficulty']}")
        print(f"         Q: {question_data['question'][:65]}...")

        # Get REAL agent prediction
        start_time = time.time()
        try:
            # Process query with real agent (correct method name)
            agent_result = agent.process_query(question_data['question'])

            # Extract SQL from agent result
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
                print(f"         Agent: ✅ Generated SQL ({execution_time:.2f}s)")
            else:
                agent_stats['failure_count'] += 1
                print(f"         Agent: ❌ Failed to generate SQL")
                predicted_sql = ""  # Empty SQL for failed generation

        except Exception as e:
            execution_time = time.time() - start_time
            agent_stats['failure_count'] += 1
            agent_stats['total_time'] += execution_time
            print(f"         Agent: ❌ Error - {str(e)[:50]}")
            predicted_sql = ""

        # Create evaluation context WITH database connection
        context = EvaluationContext(
            question_id=question_data['id'],
            question=question_data['question'],
            ground_truth_sql=question_data['query'],
            predicted_sql=predicted_sql,
            database_connection=db_connection
        )

        # Evaluate with each metric
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

                # Only add to scores if agent succeeded
                if predicted_sql.strip():
                    metric_scores[metric.name].append(result.score)

                status = "✅" if result.is_correct else "❌"
                print(f"         {metric.name[:22]:22s}: {result.score:.3f} {status}")

            except Exception as e:
                print(f"         {metric.name[:22]:22s}: ERROR - {str(e)[:30]}")
                question_results['metrics'][metric.name] = {
                    'score': 0.0,
                    'is_correct': False,
                    'error': str(e)
                }

        results.append(question_results)

        # Progress indicator
        if i % 10 == 0:
            print(f"\n{'─'*80}")
            print(f"Progress: {i}/{total_questions} ({i/total_questions*100:.1f}%) completed")
            print(f"Agent success rate so far: {agent_stats['success_count']}/{i} ({agent_stats['success_count']/i*100:.1f}%)")
            print(f"{'─'*80}")

    # Close database connection
    db_connection.close()
    print(f"\n✅ Database connection closed")

    # Calculate summary statistics
    print(f"\n{'='*80}")
    print("EVALUATION SUMMARY")
    print("="*80)

    print(f"\nAgent Performance:")
    print(f"  Success: {agent_stats['success_count']}/{total_questions} ({agent_stats['success_count']/total_questions*100:.1f}%)")
    print(f"  Failures: {agent_stats['failure_count']}/{total_questions} ({agent_stats['failure_count']/total_questions*100:.1f}%)")
    print(f"  Avg time per query: {agent_stats['total_time']/total_questions:.2f}s")
    print(f"  Total time: {agent_stats['total_time']:.1f}s")

    print(f"\nMetrics (on {agent_stats['success_count']} successful agent responses):")

    for metric_name, scores in metric_scores.items():
        if scores:
            avg_score = sum(scores) / len(scores)
            accuracy = sum(1 for s in scores if s >= 0.8) / len(scores)
            perfect_matches = sum(1 for s in scores if s == 1.0)

            print(f"\n{metric_name}:")
            print(f"  Average Score: {avg_score:.3f}")
            print(f"  Accuracy (≥0.8): {accuracy:.1%} ({int(accuracy * len(scores))}/{len(scores)})")
            print(f"  Perfect Matches: {perfect_matches}/{len(scores)} ({perfect_matches/len(scores):.1%})")
        else:
            print(f"\n{metric_name}:")
            print(f"  No successful queries to evaluate")

    # Difficulty breakdown
    print(f"\n{'─'*80}")
    print("Breakdown by Difficulty:")
    print("─"*80)

    difficulties = {}
    for r in results:
        diff = r['difficulty']
        if diff not in difficulties:
            difficulties[diff] = {'total': 0, 'agent_success': 0, 'ex_correct': 0}

        difficulties[diff]['total'] += 1
        if r['agent_success']:
            difficulties[diff]['agent_success'] += 1
            if r['metrics'].get('Execution Accuracy (EX)', {}).get('is_correct', False):
                difficulties[diff]['ex_correct'] += 1

    for diff, stats in sorted(difficulties.items()):
        total = stats['total']
        agent_success = stats['agent_success']
        ex_correct = stats['ex_correct']

        print(f"\n{diff.upper()}:")
        print(f"  Questions: {total}")
        print(f"  Agent success: {agent_success}/{total} ({agent_success/total*100:.1f}%)")
        if agent_success > 0:
            print(f"  EX accuracy: {ex_correct}/{agent_success} ({ex_correct/agent_success*100:.1f}%)")

    # Save results
    results_data = {
        'evaluation_timestamp': datetime.now().isoformat(),
        'total_questions': total_questions,
        'agent_type': 'LangGraphOrchestrator',
        'agent_config': {
            'provider': config.llm_provider,
            'model': config.llm_model
        },
        'database_connected': True,
        'execution_accuracy_enabled': True,
        'agent_stats': agent_stats,
        'summary': {
            'metric_scores': {name: sum(scores)/len(scores) if scores else 0
                            for name, scores in metric_scores.items()},
            'metric_accuracy': {name: sum(1 for s in scores if s >= 0.8)/len(scores) if scores else 0
                              for name, scores in metric_scores.items()},
            'difficulty_breakdown': difficulties
        },
        'detailed_results': results
    }

    output_path = project_root / "evaluation" / "results" / "full_evaluation_all_questions.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Results saved to: {output_path}")
    print("="*80)

    return results_data


if __name__ == "__main__":
    try:
        print(f"\n🚀 Starting full evaluation with real agent...\n")
        start_time = time.time()

        results = run_full_evaluation()

        elapsed_time = time.time() - start_time

        if results:
            print(f"\n{'='*80}")
            print("🎉 FULL EVALUATION COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"\n⏱️  Total elapsed time: {elapsed_time:.1f}s ({elapsed_time/60:.1f} minutes)")
            print(f"\n📊 Final Results:")
            print(f"   - Total questions: {results['total_questions']}")
            print(f"   - Agent success: {results['agent_stats']['success_count']}/{results['total_questions']} ({results['agent_stats']['success_count']/results['total_questions']*100:.1f}%)")

            if results['agent_stats']['success_count'] > 0:
                print(f"\n   Metrics on successful queries:")
                for metric_name, score in results['summary']['metric_scores'].items():
                    if score > 0:
                        print(f"   - {metric_name}: {score:.1%}")
        else:
            print("\n❌ Evaluation failed or was skipped")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Evaluation interrupted by user")
        print("Partial results may be available in evaluation/results/")
        sys.exit(130)

    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
