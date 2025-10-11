#!/usr/bin/env python3
"""
Database-Enabled Text-to-SQL Evaluator

This module provides database-enabled evaluation using the same connection
pattern as the existing evaluation scripts, ensuring compatibility and
enabling full Execution Accuracy (EX) metric functionality.
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(dotenv_path=project_root / ".env")

from evaluation.metrics import ExactMatchMetric, ComponentMatchingMetric, ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext
from evaluation.evaluator import TextToSQLEvaluator, QuestionSample
from src.application.config.simple_config import ApplicationConfig


class DatabaseConnection:
    """Database connection wrapper compatible with existing infrastructure"""

    def __init__(self, connection_uri: str):
        self.connection_uri = connection_uri
        self._engine = None
        self._setup_connection()

    def _setup_connection(self):
        """Setup SQLAlchemy connection"""
        from sqlalchemy import create_engine

        # Convert psycopg2 URI to standard PostgreSQL URI for compatibility
        uri = self.connection_uri
        if uri.startswith("postgresql+psycopg2://"):
            uri = uri.replace("postgresql+psycopg2://", "postgresql://", 1)

        self._engine = create_engine(uri)

    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            from sqlalchemy import text
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            return False

    def get_raw_connection(self):
        """Get raw connection for direct query execution"""
        return self._engine.raw_connection()

    def execute_query(self, sql_query: str) -> tuple:
        """
        Execute SQL query and return results

        Returns:
            Tuple of (results, error_message)
        """
        try:
            from sqlalchemy import text

            with self._engine.connect() as conn:
                # Set timeout
                conn.execute(text("SET statement_timeout = 30000"))  # 30 seconds

                # Execute query
                result = conn.execute(text(sql_query))

                # Fetch results
                try:
                    rows = result.fetchall()
                    # Convert to list of tuples for compatibility
                    return [tuple(row) for row in rows], None
                except Exception:
                    # Query doesn't return results (e.g., INSERT, UPDATE)
                    return [], None

        except Exception as e:
            return None, str(e)


def ensure_database_available(connection_uri: str) -> bool:
    """Check if database is available, following existing pattern"""
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError

    if connection_uri.startswith("postgresql+psycopg2://"):
        connection_uri = connection_uri.replace("postgresql+psycopg2://", "postgresql://", 1)

    try:
        engine = create_engine(connection_uri)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"[evaluation] Database connection successful: {connection_uri}")
        return True
    except SQLAlchemyError as exc:
        print(f"[evaluation] Database connection failed: {exc}")
        return False


class DatabaseTextToSQLEvaluator(TextToSQLEvaluator):
    """Enhanced evaluator with database integration"""

    def __init__(self, config: Optional[ApplicationConfig] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize database-enabled evaluator

        Args:
            config: Application configuration (uses default if None)
            logger: Logger instance (creates one if None)
        """
        self.config = config or ApplicationConfig()
        self.logger = logger or self._create_logger()

        # Setup database connection
        self.db_connection = self._setup_database_connection()

        # Initialize metrics (all three including EX)
        if self.db_connection:
            metrics = [
                ExactMatchMetric(),
                ComponentMatchingMetric(),
                ExecutionAccuracyMetric(execution_timeout=30)
            ]
            self.logger.info("✅ All three metrics initialized (EM, CM, EX)")
        else:
            metrics = [
                ExactMatchMetric(),
                ComponentMatchingMetric()
            ]
            self.logger.warning("⚠️ Only EM and CM metrics initialized (no database)")

        # Initialize parent with database connection
        super().__init__(
            database_connection=self.db_connection,
            metrics=metrics,
            logger=self.logger
        )

    def _setup_database_connection(self) -> Optional[DatabaseConnection]:
        """Setup database connection using config"""
        if not self.config.database_path:
            self.logger.warning("No database path configured")
            return None

        self.logger.info(f"Attempting database connection: {self.config.database_path}")

        if not ensure_database_available(self.config.database_path):
            self.logger.error("Database connection failed")
            return None

        try:
            connection = DatabaseConnection(self.config.database_path)
            if connection.test_connection():
                self.logger.info("✅ Database connection established")
                return connection
            else:
                self.logger.error("Database connection test failed")
                return None
        except Exception as e:
            self.logger.error(f"Failed to create database connection: {e}")
            return None

    def evaluate_with_agent(
        self,
        agent_orchestrator,
        ground_truth_path: str = "evaluation/ground_truth.json",
        sample_size: int = 10,
        difficulty_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Evaluate agent with database-enabled metrics

        Args:
            agent_orchestrator: Agent to evaluate
            ground_truth_path: Path to ground truth file
            sample_size: Number of questions to evaluate
            difficulty_filter: Filter by difficulty levels

        Returns:
            Complete evaluation results including EX metrics
        """
        self.logger.info(f"Starting database-enabled evaluation with {len(self.metrics)} metrics")

        # Load and sample questions
        questions = self._load_and_sample_questions(
            Path(ground_truth_path), sample_size, difficulty_filter, random_seed=42
        )

        results = []
        start_time = datetime.now()

        for i, question in enumerate(questions, 1):
            self.logger.info(f"[{i:2d}/{len(questions)}] Evaluating: {question.id}")

            try:
                # Get agent prediction
                agent_response = agent_orchestrator.process_request(question.question)
                predicted_sql = self._extract_sql_from_response(agent_response)

                if not predicted_sql:
                    self.logger.warning(f"No SQL generated for {question.id}")
                    results.append({
                        'question_id': question.id,
                        'question': question.question,
                        'ground_truth_sql': question.ground_truth_sql,
                        'predicted_sql': '',
                        'agent_success': False,
                        'metrics': {},
                        'error': 'No SQL generated'
                    })
                    continue

                # Evaluate with all metrics
                metric_results = self.evaluate_single_prediction(
                    question.question,
                    question.ground_truth_sql,
                    predicted_sql,
                    question.id
                )

                # Convert MetricResult objects to dicts
                metrics_dict = {}
                for metric_name, result in metric_results.items():
                    metrics_dict[metric_name] = {
                        'score': result.score,
                        'is_correct': result.is_correct,
                        'error_message': result.error_message,
                        'details': result.details
                    }

                results.append({
                    'question_id': question.id,
                    'question': question.question,
                    'ground_truth_sql': question.ground_truth_sql,
                    'predicted_sql': predicted_sql,
                    'agent_success': True,
                    'metrics': metrics_dict
                })

                # Log results
                for metric_name, result_dict in metrics_dict.items():
                    status = "✅" if result_dict['is_correct'] else "❌"
                    self.logger.info(f"        {metric_name}: {result_dict['score']:.3f} {status}")

            except Exception as e:
                self.logger.error(f"Error evaluating {question.id}: {e}")
                results.append({
                    'question_id': question.id,
                    'question': question.question,
                    'ground_truth_sql': question.ground_truth_sql,
                    'predicted_sql': '',
                    'agent_success': False,
                    'metrics': {},
                    'error': str(e)
                })

        # Generate summary
        total_time = (datetime.now() - start_time).total_seconds()
        summary = self._generate_evaluation_summary(results, total_time)

        return {
            'summary': summary,
            'individual_results': results,
            'config': {
                'sample_size': sample_size,
                'difficulty_filter': difficulty_filter,
                'database_enabled': self.db_connection is not None,
                'metrics_count': len(self.metrics)
            }
        }

    def _generate_evaluation_summary(self, results: List[Dict], total_time: float) -> Dict[str, Any]:
        """Generate evaluation summary with all metrics"""
        total_questions = len(results)
        successful_evaluations = sum(1 for r in results if r['agent_success'])

        # Calculate metric averages
        metric_averages = {}
        metric_accuracies = {}

        # Get all metric names from successful evaluations
        all_metrics = set()
        for result in results:
            if result['agent_success']:
                all_metrics.update(result['metrics'].keys())

        for metric_name in all_metrics:
            scores = []
            correct_count = 0

            for result in results:
                if result['agent_success'] and metric_name in result['metrics']:
                    metric_data = result['metrics'][metric_name]
                    scores.append(metric_data['score'])
                    if metric_data['is_correct']:
                        correct_count += 1
                else:
                    scores.append(0.0)

            metric_averages[metric_name] = sum(scores) / len(scores) if scores else 0.0
            metric_accuracies[metric_name] = correct_count / total_questions if total_questions > 0 else 0.0

        return {
            'total_questions': total_questions,
            'successful_evaluations': successful_evaluations,
            'agent_success_rate': successful_evaluations / total_questions if total_questions > 0 else 0.0,
            'metric_scores': metric_averages,
            'metric_accuracies': metric_accuracies,
            'evaluation_time_seconds': total_time,
            'database_enabled': self.db_connection is not None,
            'timestamp': datetime.now().isoformat()
        }


def run_database_evaluation(
    sample_size: int = 10,
    difficulty_filter: Optional[List[str]] = None,
    output_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run complete evaluation with database integration

    Args:
        sample_size: Number of questions to evaluate
        difficulty_filter: Filter by difficulty (e.g., ['easy', 'medium'])
        output_file: Path to save results (optional)

    Returns:
        Complete evaluation results
    """
    print("="*60)
    print("DATABASE-ENABLED TEXT-TO-SQL EVALUATION")
    print("="*60)

    # Create evaluator
    evaluator = DatabaseTextToSQLEvaluator()

    # Create mock agent for testing
    class MockAgent:
        def process_request(self, question: str) -> str:
            # Simple mock responses for testing
            if "quantas internações" in question.lower():
                return "SELECT COUNT(*) AS total_internacoes FROM internacoes"
            elif "quantas mortes" in question.lower():
                return "SELECT COUNT(*) AS total_mortes FROM mortes"
            elif "procedimentos" in question.lower():
                return "SELECT COUNT(*) AS total_procedimentos FROM procedimentos"
            elif "cid" in question.lower():
                return "SELECT COUNT(*) AS total_cids FROM cid10"
            elif "hospitais" in question.lower():
                return "SELECT COUNT(*) AS total_hospitais FROM hospital"
            else:
                return "SELECT COUNT(*) FROM internacoes"

    agent = MockAgent()

    # Run evaluation
    results = evaluator.evaluate_with_agent(
        agent_orchestrator=agent,
        sample_size=sample_size,
        difficulty_filter=difficulty_filter
    )

    # Display results
    summary = results['summary']
    print(f"\n📊 EVALUATION RESULTS:")
    print(f"Total Questions: {summary['total_questions']}")
    print(f"Agent Success Rate: {summary['agent_success_rate']:.1%}")
    print(f"Database Enabled: {summary['database_enabled']}")
    print(f"Evaluation Time: {summary['evaluation_time_seconds']:.1f}s")

    print(f"\n📈 METRIC SCORES:")
    for metric_name, score in summary['metric_scores'].items():
        accuracy = summary['metric_accuracies'].get(metric_name, 0)
        print(f"{metric_name:25s}: {score:.3f} (accuracy: {accuracy:.1%})")

    # Save results if requested
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        print(f"\n💾 Results saved to: {output_path}")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run database-enabled Text-to-SQL evaluation")
    parser.add_argument("--sample-size", "-n", type=int, default=10, help="Number of questions to evaluate")
    parser.add_argument("--difficulty", "-d", nargs="*", choices=["easy", "medium", "hard"], help="Filter by difficulty")
    parser.add_argument("--output", "-o", type=str, help="Output file path")

    args = parser.parse_args()

    results = run_database_evaluation(
        sample_size=args.sample_size,
        difficulty_filter=args.difficulty,
        output_file=args.output
    )

    print("\n🎉 Database evaluation completed successfully!")