#!/usr/bin/env python3
"""
Evaluation with Execution Accuracy (EX) Metric

This script runs evaluation including the EX metric by connecting to the database
and executing both ground truth and predicted SQL queries.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext


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


class MockAgentOrchestrator:
    """Mock agent for testing - generates realistic SQL variations"""

    def __init__(self):
        self.question_count = 0

    def process_request(self, question: str) -> str:
        """Generate mock SQL responses with realistic variations"""
        self.question_count += 1

        # Mock some realistic agent behaviors
        if "quantas internações" in question.lower():
            return "SELECT COUNT(*) FROM internacoes;"
        elif "quantas mortes" in question.lower():
            return "SELECT COUNT(*) FROM mortes;"
        elif "procedimentos" in question.lower():
            return "SELECT COUNT(*) FROM procedimentos;"
        elif "cid" in question.lower():
            return "SELECT COUNT(*) FROM cid10;"
        elif "hospitais" in question.lower():
            return "SELECT COUNT(*) FROM hospital;"
        else:
            # Default response
            return "SELECT COUNT(*) FROM internacoes;"


def load_ground_truth_sample(file_path: Path, sample_size: int = 5) -> list:
    """Load a sample from ground truth data"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Take first N questions for testing
    return data[:sample_size]


def run_evaluation_with_ex():
    """Run evaluation with Execution Accuracy metric"""
    print("="*70)
    print("TEXT-TO-SQL EVALUATION WITH EXECUTION ACCURACY (EX)")
    print("="*70)

    # Check for database URL (try both DATABASE_URL and DATABASE_PATH)
    db_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH")
    if not db_url:
        print("❌ ERROR: DATABASE_URL or DATABASE_PATH not found in environment")
        print("   Please set DATABASE_URL or DATABASE_PATH in .env file")
        print("   Example: DATABASE_URL=postgresql://user:pass@localhost/dbname")
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

    # Load ground truth sample
    gt_path = project_root / "evaluation" / "ground_truth.json"
    if not gt_path.exists():
        print(f"❌ Ground truth file not found: {gt_path}")
        return None

    sample_size = 5
    sample_questions = load_ground_truth_sample(gt_path, sample_size)
    print(f"✅ Loaded {len(sample_questions)} questions from ground truth")

    # Initialize metrics (ALL THREE including EX)
    metrics = [
        ExactMatchMetric(),
        ComponentMatchingMetric(),
        ExecutionAccuracyMetric(execution_timeout=30)
    ]
    print(f"✅ Initialized {len(metrics)} metrics (including EX)")

    # Mock agent
    agent = MockAgentOrchestrator()

    # Results storage
    results = []
    metric_scores = {metric.name: [] for metric in metrics}

    print("\n" + "="*70)
    print("EVALUATING QUESTIONS (WITH DATABASE EXECUTION)")
    print("="*70)

    # Evaluate each question
    for i, question_data in enumerate(sample_questions, 1):
        print(f"\n[{i:2d}/{sample_size}] Question: {question_data['id']}")
        print(f"        Text: {question_data['question'][:60]}...")

        # Get agent prediction
        predicted_sql = agent.process_request(question_data['question'])

        # Create evaluation context WITH database connection
        context = EvaluationContext(
            question_id=question_data['id'],
            question=question_data['question'],
            ground_truth_sql=question_data['query'],
            predicted_sql=predicted_sql,
            database_connection=db_connection  # ← DATABASE CONNECTION PROVIDED
        )

        # Evaluate with each metric
        question_results = {
            'question_id': question_data['id'],
            'question': question_data['question'],
            'ground_truth_sql': question_data['query'],
            'predicted_sql': predicted_sql,
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
                metric_scores[metric.name].append(result.score)

                status = "✅" if result.is_correct else "❌"
                print(f"        {metric.name}: {result.score:.3f} {status}")

                # Show EX details
                if metric.name == "Execution Accuracy (EX)" and result.details:
                    details = result.details
                    if 'ground_truth_rows' in details:
                        print(f"          GT rows: {details['ground_truth_rows']}, Pred rows: {details.get('predicted_rows', 0)}")
                    if 'reason' in details:
                        print(f"          Reason: {details['reason']}")

            except Exception as e:
                print(f"        {metric.name}: ERROR - {e}")
                question_results['metrics'][metric.name] = {
                    'score': 0.0,
                    'is_correct': False,
                    'error': str(e)
                }

        results.append(question_results)

    # Close database connection
    db_connection.close()
    print(f"\n✅ Database connection closed")

    # Calculate summary statistics
    print("\n" + "="*70)
    print("EVALUATION SUMMARY")
    print("="*70)

    total_questions = len(sample_questions)

    for metric_name, scores in metric_scores.items():
        if scores:
            avg_score = sum(scores) / len(scores)
            accuracy = sum(1 for s in scores if s >= 0.8) / len(scores)
            perfect_matches = sum(1 for s in scores if s == 1.0)

            print(f"\n{metric_name}:")
            print(f"  Average Score: {avg_score:.3f}")
            print(f"  Accuracy (≥0.8): {accuracy:.1%} ({int(accuracy * len(scores))}/{len(scores)})")
            print(f"  Perfect Matches: {perfect_matches}/{len(scores)} ({perfect_matches/len(scores):.1%})")

    # Save results
    results_data = {
        'evaluation_timestamp': datetime.now().isoformat(),
        'total_questions': total_questions,
        'database_connected': True,
        'execution_accuracy_enabled': True,
        'summary': {
            'metric_scores': {name: sum(scores)/len(scores) if scores else 0
                            for name, scores in metric_scores.items()},
            'metric_accuracy': {name: sum(1 for s in scores if s >= 0.8)/len(scores) if scores else 0
                              for name, scores in metric_scores.items()}
        },
        'detailed_results': results
    }

    output_path = project_root / "evaluation" / "results" / "evaluation_with_ex.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Results saved to: {output_path}")
    print("="*70)

    return results_data


if __name__ == "__main__":
    try:
        results = run_evaluation_with_ex()
        if results:
            print("\n🎉 Evaluation with Execution Accuracy completed successfully!")
            print("\n📊 Key Findings:")
            print(f"   - EM: {results['summary']['metric_scores']['Exact Match (EM)']:.1%}")
            print(f"   - CM: {results['summary']['metric_scores']['Component Matching (CM)']:.1%}")
            print(f"   - EX: {results['summary']['metric_scores']['Execution Accuracy (EX)']:.1%}")
        else:
            print("\n❌ Evaluation failed or was skipped")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
