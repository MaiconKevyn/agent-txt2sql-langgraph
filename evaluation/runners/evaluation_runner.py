#!/usr/bin/env python3
"""
CLI Evaluation Runner for Text-to-SQL Metrics

This script provides a command-line interface for running comprehensive
evaluations of Text-to-SQL systems using EM, CM, and EX metrics.

Usage:
    python -m evaluation.runners.evaluation_runner \
        --ground-truth evaluation/ground_truth.json \
        --output results/evaluation_results.json \
        --sample-size 10 \
        --config-path src/application/config/simple_config.yaml
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from evaluation.evaluator import TextToSQLEvaluator
from evaluation.metrics import ExactMatchMetric, ComponentMatchingMetric, ExecutionAccuracyMetric
from src.infrastructure.database.connection_service import DatabaseConnectionFactory
from src.application.config.simple_config import ApplicationConfig
from src.agent.orchestrator import LangGraphOrchestrator


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('evaluation.log')
        ]
    )
    return logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run comprehensive Text-to-SQL evaluation with EM, CM, and EX metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic evaluation with 10 samples
  python -m evaluation.runners.evaluation_runner \\
      --ground-truth evaluation/ground_truth.json \\
      --output results/eval_results.json

  # Evaluation with specific difficulty levels
  python -m evaluation.runners.evaluation_runner \\
      --ground-truth evaluation/ground_truth.json \\
      --output results/eval_results.json \\
      --sample-size 20 \\
      --difficulty easy medium

  # Evaluation with custom database config
  python -m evaluation.runners.evaluation_runner \\
      --ground-truth evaluation/ground_truth.json \\
      --output results/eval_results.json \\
      --db-url "postgresql://user:pass@localhost/dbname"
        """
    )

    # Required arguments
    parser.add_argument(
        "--ground-truth", "-gt",
        type=Path,
        required=True,
        help="Path to ground truth JSON file"
    )

    parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Path for output results JSON file"
    )

    # Optional arguments
    parser.add_argument(
        "--sample-size", "-n",
        type=int,
        default=10,
        help="Number of questions to sample for evaluation (default: 10)"
    )

    parser.add_argument(
        "--difficulty", "-d",
        nargs="*",
        choices=["easy", "medium", "hard"],
        help="Filter questions by difficulty level (default: all difficulties)"
    )

    parser.add_argument(
        "--random-seed", "-s",
        type=int,
        help="Random seed for reproducible sampling"
    )

    parser.add_argument(
        "--config-path", "-c",
        type=Path,
        help="Path to application config file (optional)"
    )

    parser.add_argument(
        "--db-url",
        type=str,
        help="Database connection URL (overrides config file)"
    )

    parser.add_argument(
        "--metrics",
        nargs="*",
        choices=["em", "cm", "ex"],
        default=["em", "cm", "ex"],
        help="Metrics to run (default: all)"
    )

    parser.add_argument(
        "--log-level", "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="SQL execution timeout in seconds (default: 30)"
    )

    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Generate report from existing results file without running evaluation"
    )

    return parser.parse_args()


def load_config(config_path: Optional[Path]) -> Optional[ApplicationConfig]:
    """Load application configuration"""
    if not config_path or not config_path.exists():
        return None

    try:
        return ApplicationConfig.from_yaml(config_path)
    except Exception as e:
        logging.warning(f"Failed to load config from {config_path}: {e}")
        return None


def create_database_connection(db_url: Optional[str], config: Optional[ApplicationConfig]):
    """Create database connection service"""
    if db_url:
        return DatabaseConnectionFactory.create_postgresql_service(db_url)
    elif config and hasattr(config, 'database_config'):
        return DatabaseConnectionFactory.create_postgresql_service(config.database_config.connection_string)
    else:
        # Default connection - you may need to adjust this
        default_url = "postgresql://user:password@localhost:5432/datasus"
        logging.warning(f"Using default database URL: {default_url}")
        return DatabaseConnectionFactory.create_postgresql_service(default_url)


def create_metrics(metric_names: list, timeout: int) -> list:
    """Create metric instances based on names"""
    metrics = []
    metric_map = {
        "em": ExactMatchMetric,
        "cm": ComponentMatchingMetric,
        "ex": lambda: ExecutionAccuracyMetric(execution_timeout=timeout)
    }

    for name in metric_names:
        if name in metric_map:
            metric_class = metric_map[name]
            metrics.append(metric_class())
        else:
            logging.warning(f"Unknown metric: {name}")

    return metrics


def create_agent_orchestrator(config: Optional[ApplicationConfig]):
    """Create agent orchestrator for evaluation"""
    try:
        if config:
            return LangGraphOrchestrator(app_config=config)
        else:
            # Create with minimal config
            return LangGraphOrchestrator()
    except Exception as e:
        logging.error(f"Failed to create agent orchestrator: {e}")
        raise


def generate_report(results: dict, output_path: Path, logger: logging.Logger):
    """Generate and display evaluation report"""
    summary = results.get('summary', {})

    logger.info("\n" + "="*60)
    logger.info("TEXT-TO-SQL EVALUATION REPORT")
    logger.info("="*60)

    # Basic statistics
    logger.info(f"Total Questions: {summary.get('total_questions', 0)}")
    logger.info(f"Agent Success Rate: {summary.get('agent_success_rate', 0):.1%}")
    logger.info(f"Evaluation Time: {summary.get('execution_time_total_ms', 0)/1000:.1f}s")

    # Metric scores
    logger.info("\nMETRIC SCORES:")
    logger.info("-" * 40)
    metric_scores = summary.get('metric_scores', {})
    metric_accuracy = summary.get('metric_accuracy', {})

    for metric_name in metric_scores:
        score = metric_scores[metric_name]
        accuracy = metric_accuracy.get(metric_name, 0)
        logger.info(f"{metric_name:20s}: {score:.3f} (accuracy: {accuracy:.1%})")

    # Difficulty breakdown
    difficulty_breakdown = summary.get('difficulty_breakdown', {})
    if difficulty_breakdown:
        logger.info("\nDIFFICULTY BREAKDOWN:")
        logger.info("-" * 40)
        for difficulty, stats in difficulty_breakdown.items():
            logger.info(f"{difficulty.upper()}:")
            logger.info(f"  Total questions: {stats.get('total_questions', 0)}")
            logger.info(f"  Success rate: {stats.get('agent_success_rate', 0):.1%}")

            for metric_name in metric_scores:
                score_key = f"{metric_name}_score"
                accuracy_key = f"{metric_name}_accuracy"
                if score_key in stats:
                    score = stats[score_key]
                    accuracy = stats.get(accuracy_key, 0)
                    logger.info(f"  {metric_name}: {score:.3f} ({accuracy:.1%})")

    # Individual results summary
    individual_results = results.get('individual_results', [])
    if individual_results:
        logger.info(f"\nINDIVIDUAL RESULTS:")
        logger.info("-" * 40)

        for result in individual_results:
            question_id = result.get('question_id', 'unknown')
            agent_success = result.get('agent_success', False)
            status = "✅" if agent_success else "❌"

            logger.info(f"{status} {question_id}: Agent {'SUCCESS' if agent_success else 'FAILED'}")

            if agent_success:
                metric_results = result.get('metric_results', {})
                metric_line = []
                for metric_name, metric_result in metric_results.items():
                    if isinstance(metric_result, dict):
                        score = metric_result.get('score', 0)
                        is_correct = metric_result.get('is_correct', False)
                        status_symbol = "✅" if is_correct else "❌"
                        metric_line.append(f"{metric_name}: {score:.3f}{status_symbol}")

                if metric_line:
                    logger.info(f"    {' | '.join(metric_line)}")

    logger.info(f"\nDetailed results saved to: {output_path}")
    logger.info("="*60)


def main():
    """Main entry point"""
    args = parse_arguments()
    logger = setup_logging(args.log_level)

    try:
        # Validate input files
        if not args.ground_truth.exists():
            logger.error(f"Ground truth file not found: {args.ground_truth}")
            sys.exit(1)

        # Handle report-only mode
        if args.report_only:
            if not args.output.exists():
                logger.error(f"Results file not found for report: {args.output}")
                sys.exit(1)

            with open(args.output, 'r') as f:
                results = json.load(f)
            generate_report(results, args.output, logger)
            return

        # Load configuration
        config = load_config(args.config_path)
        if config:
            logger.info(f"Loaded configuration from {args.config_path}")

        # Create database connection
        logger.info("Setting up database connection...")
        db_connection = create_database_connection(args.db_url, config)

        # Test database connection
        if not db_connection.test_connection():
            logger.error("Database connection test failed")
            sys.exit(1)
        logger.info("Database connection established")

        # Create metrics
        metrics = create_metrics(args.metrics, args.timeout)
        logger.info(f"Initialized {len(metrics)} metrics: {[m.name for m in metrics]}")

        # Create evaluator
        evaluator = TextToSQLEvaluator(
            database_connection=db_connection,
            metrics=metrics,
            logger=logger
        )

        # Create agent orchestrator
        logger.info("Initializing agent orchestrator...")
        agent_orchestrator = create_agent_orchestrator(config)

        # Run evaluation
        logger.info("Starting evaluation...")
        results = evaluator.evaluate_sample(
            ground_truth_path=args.ground_truth,
            agent_orchestrator=agent_orchestrator,
            sample_size=args.sample_size,
            difficulty_filter=args.difficulty,
            random_seed=args.random_seed
        )

        # Save results
        evaluator.save_results(results, args.output)

        # Generate report
        generate_report(results, args.output, logger)

        logger.info("Evaluation completed successfully!")

    except KeyboardInterrupt:
        logger.info("Evaluation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Evaluation failed with error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()