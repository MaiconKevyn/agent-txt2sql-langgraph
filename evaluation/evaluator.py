"""
Main Evaluation Orchestrator for Text-to-SQL Metrics

This module provides the primary interface for evaluating Text-to-SQL systems
using multiple metrics including Exact Match (EM), Component Matching (CM),
and Execution Accuracy (EX).

The orchestrator handles:
- Multiple metric evaluation
- Sample selection from ground truth
- Agent integration
- Comprehensive reporting
- Error handling and recovery
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import random

from .metrics import (
    BaseMetric, MetricResult, EvaluationContext,
    ExactMatchMetric, ComponentMatchingMetric, ExecutionAccuracyMetric
)
from src.infrastructure.database.connection_service import IDatabaseConnectionService


@dataclass
class QuestionSample:
    """Represents a single evaluation question"""
    id: str
    question: str
    ground_truth_sql: str
    difficulty: str
    tables: List[str]
    notes: Optional[str] = None


@dataclass
class EvaluationResult:
    """Complete evaluation result for a single question"""
    question_id: str
    question: str
    ground_truth_sql: str
    predicted_sql: str
    agent_success: bool
    metric_results: Dict[str, MetricResult]
    execution_time_ms: Optional[float] = None
    agent_error: Optional[str] = None


@dataclass
class EvaluationSummary:
    """Summary statistics across all evaluated questions"""
    total_questions: int
    agent_success_rate: float
    metric_scores: Dict[str, float]  # Average scores per metric
    metric_accuracy: Dict[str, float]  # Accuracy (binary) per metric
    difficulty_breakdown: Dict[str, Dict[str, float]]
    evaluation_timestamp: str
    execution_time_total_ms: float


class TextToSQLEvaluator:
    """
    Main orchestrator for Text-to-SQL evaluation

    Provides comprehensive evaluation using multiple metrics and integrates
    with the agent system for end-to-end testing.
    """

    def __init__(
        self,
        database_connection: IDatabaseConnectionService,
        metrics: Optional[List[BaseMetric]] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the evaluator

        Args:
            database_connection: Database connection for execution accuracy
            metrics: List of metrics to use (defaults to all three)
            logger: Logger instance (creates one if None)
        """
        self.database_connection = database_connection
        self.logger = logger or self._create_logger()

        # Initialize metrics
        if metrics is None:
            self.metrics = [
                ExactMatchMetric(),
                ComponentMatchingMetric(),
                ExecutionAccuracyMetric()
            ]
        else:
            self.metrics = metrics

        self.logger.info(f"Initialized evaluator with {len(self.metrics)} metrics")

    def evaluate_sample(
        self,
        ground_truth_path: Union[str, Path],
        agent_orchestrator,
        sample_size: int = 10,
        difficulty_filter: Optional[List[str]] = None,
        random_seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Evaluate agent on a sample of questions from ground truth

        Args:
            ground_truth_path: Path to ground truth JSON file
            agent_orchestrator: Agent to evaluate
            sample_size: Number of questions to sample
            difficulty_filter: Filter by difficulty levels (e.g., ['easy', 'medium'])
            random_seed: Random seed for reproducible sampling

        Returns:
            Complete evaluation results with summary
        """
        self.logger.info(f"Starting evaluation with sample size {sample_size}")

        # Load and sample questions
        questions = self._load_and_sample_questions(
            ground_truth_path, sample_size, difficulty_filter, random_seed
        )

        self.logger.info(f"Loaded {len(questions)} questions for evaluation")

        # Evaluate each question
        evaluation_results = []
        start_time = datetime.now()

        for i, question in enumerate(questions):
            self.logger.info(f"Evaluating question {i+1}/{len(questions)}: {question.id}")

            result = self._evaluate_single_question(question, agent_orchestrator)
            evaluation_results.append(result)

            # Log intermediate progress
            if (i + 1) % 5 == 0:
                self.logger.info(f"Completed {i+1}/{len(questions)} evaluations")

        end_time = datetime.now()
        total_time_ms = (end_time - start_time).total_seconds() * 1000

        # Generate summary
        summary = self._generate_summary(evaluation_results, total_time_ms)

        # Prepare complete results
        complete_results = {
            'summary': asdict(summary),
            'individual_results': [asdict(result) for result in evaluation_results],
            'evaluation_config': {
                'sample_size': sample_size,
                'difficulty_filter': difficulty_filter,
                'random_seed': random_seed,
                'metrics_used': [metric.name for metric in self.metrics]
            }
        }

        self.logger.info("Evaluation completed successfully")
        return complete_results

    def evaluate_single_prediction(
        self,
        question: str,
        ground_truth_sql: str,
        predicted_sql: str,
        question_id: str = "single"
    ) -> Dict[str, MetricResult]:
        """
        Evaluate a single prediction against ground truth

        Args:
            question: Natural language question
            ground_truth_sql: Ground truth SQL query
            predicted_sql: Predicted SQL query
            question_id: Identifier for the question

        Returns:
            Dictionary of metric results
        """
        context = EvaluationContext(
            question_id=question_id,
            question=question,
            ground_truth_sql=ground_truth_sql,
            predicted_sql=predicted_sql,
            database_connection=self.database_connection
        )

        results = {}
        for metric in self.metrics:
            try:
                result = metric.evaluate(context)
                results[metric.name] = result
                self.logger.debug(f"{metric.name}: {result.score:.3f}")
            except Exception as e:
                self.logger.error(f"Error in {metric.name}: {e}")
                results[metric.name] = MetricResult(
                    metric_name=metric.name,
                    score=0.0,
                    is_correct=False,
                    details={'error': str(e)},
                    error_message=str(e)
                )

        return results

    def _evaluate_single_question(
        self,
        question: QuestionSample,
        agent_orchestrator
    ) -> EvaluationResult:
        """Evaluate a single question using the agent"""
        start_time = datetime.now()

        try:
            # Generate prediction using agent
            agent_response = agent_orchestrator.process_request(question.question)

            # Extract SQL from agent response
            predicted_sql = self._extract_sql_from_response(agent_response)

            if not predicted_sql:
                # Agent failed to generate SQL
                return EvaluationResult(
                    question_id=question.id,
                    question=question.question,
                    ground_truth_sql=question.ground_truth_sql,
                    predicted_sql="",
                    agent_success=False,
                    metric_results={},
                    agent_error="No SQL generated by agent"
                )

            # Evaluate with metrics
            metric_results = self.evaluate_single_prediction(
                question.question,
                question.ground_truth_sql,
                predicted_sql,
                question.id
            )

            execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000

            return EvaluationResult(
                question_id=question.id,
                question=question.question,
                ground_truth_sql=question.ground_truth_sql,
                predicted_sql=predicted_sql,
                agent_success=True,
                metric_results=metric_results,
                execution_time_ms=execution_time_ms
            )

        except Exception as e:
            self.logger.error(f"Error evaluating question {question.id}: {e}")

            execution_time_ms = (datetime.now() - start_time).total_seconds() * 1000

            return EvaluationResult(
                question_id=question.id,
                question=question.question,
                ground_truth_sql=question.ground_truth_sql,
                predicted_sql="",
                agent_success=False,
                metric_results={},
                execution_time_ms=execution_time_ms,
                agent_error=str(e)
            )

    def _load_and_sample_questions(
        self,
        ground_truth_path: Union[str, Path],
        sample_size: int,
        difficulty_filter: Optional[List[str]],
        random_seed: Optional[int]
    ) -> List[QuestionSample]:
        """Load questions from ground truth and sample them"""

        # Load ground truth data
        with open(ground_truth_path, 'r', encoding='utf-8') as f:
            ground_truth_data = json.load(f)

        # Convert to QuestionSample objects
        questions = []
        for item in ground_truth_data:
            if difficulty_filter and item.get('difficulty') not in difficulty_filter:
                continue

            questions.append(QuestionSample(
                id=item['id'],
                question=item['question'],
                ground_truth_sql=item['query'],
                difficulty=item.get('difficulty', 'unknown'),
                tables=item.get('tables', []),
                notes=item.get('notes')
            ))

        # Sample questions
        if sample_size >= len(questions):
            return questions

        if random_seed is not None:
            random.seed(random_seed)

        return random.sample(questions, sample_size)

    def _extract_sql_from_response(self, agent_response) -> str:
        """Extract SQL query from agent response"""
        # This depends on your agent's response format
        # Adjust based on how your agent returns SQL

        if isinstance(agent_response, dict):
            # Try common keys
            for key in ['sql', 'query', 'generated_sql', 'final_query']:
                if key in agent_response:
                    return agent_response[key]

            # Try to find SQL in nested structures
            if 'result' in agent_response:
                result = agent_response['result']
                if isinstance(result, dict):
                    for key in ['sql', 'query', 'generated_sql']:
                        if key in result:
                            return result[key]
                elif isinstance(result, str):
                    return result

        elif isinstance(agent_response, str):
            return agent_response

        # If no SQL found, log warning
        self.logger.warning(f"Could not extract SQL from agent response: {type(agent_response)}")
        return ""

    def _generate_summary(
        self,
        results: List[EvaluationResult],
        total_time_ms: float
    ) -> EvaluationSummary:
        """Generate summary statistics from evaluation results"""

        total_questions = len(results)
        successful_predictions = sum(1 for r in results if r.agent_success)

        # Calculate metric scores and accuracy
        metric_scores = {}
        metric_accuracy = {}

        for metric in self.metrics:
            scores = []
            correct_count = 0

            for result in results:
                if result.agent_success and metric.name in result.metric_results:
                    metric_result = result.metric_results[metric.name]
                    scores.append(metric_result.score)
                    if metric_result.is_correct:
                        correct_count += 1
                else:
                    scores.append(0.0)

            metric_scores[metric.name] = sum(scores) / len(scores) if scores else 0.0
            metric_accuracy[metric.name] = correct_count / total_questions if total_questions > 0 else 0.0

        # Difficulty breakdown
        difficulty_breakdown = {}
        difficulty_groups = {}

        for result in results:
            # Find difficulty from original question data (you might need to adjust this)
            difficulty = "unknown"  # Default

            if difficulty not in difficulty_groups:
                difficulty_groups[difficulty] = []
            difficulty_groups[difficulty].append(result)

        for difficulty, group_results in difficulty_groups.items():
            group_summary = {}
            group_total = len(group_results)
            group_successful = sum(1 for r in group_results if r.agent_success)

            group_summary['total_questions'] = group_total
            group_summary['agent_success_rate'] = group_successful / group_total if group_total > 0 else 0.0

            for metric in self.metrics:
                group_scores = []
                group_correct = 0

                for result in group_results:
                    if result.agent_success and metric.name in result.metric_results:
                        metric_result = result.metric_results[metric.name]
                        group_scores.append(metric_result.score)
                        if metric_result.is_correct:
                            group_correct += 1
                    else:
                        group_scores.append(0.0)

                group_summary[f'{metric.name}_score'] = sum(group_scores) / len(group_scores) if group_scores else 0.0
                group_summary[f'{metric.name}_accuracy'] = group_correct / group_total if group_total > 0 else 0.0

            difficulty_breakdown[difficulty] = group_summary

        return EvaluationSummary(
            total_questions=total_questions,
            agent_success_rate=successful_predictions / total_questions if total_questions > 0 else 0.0,
            metric_scores=metric_scores,
            metric_accuracy=metric_accuracy,
            difficulty_breakdown=difficulty_breakdown,
            evaluation_timestamp=datetime.now().isoformat(),
            execution_time_total_ms=total_time_ms
        )

    def save_results(self, results: Dict[str, Any], output_path: Union[str, Path]) -> None:
        """Save evaluation results to JSON file"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        self.logger.info(f"Results saved to {output_path}")

    def _create_logger(self) -> logging.Logger:
        """Create a default logger for the evaluator"""
        logger = logging.getLogger('TextToSQLEvaluator')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger