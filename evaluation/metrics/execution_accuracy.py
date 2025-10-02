"""
Execution Accuracy (EX) Metric Implementation

This module implements the Execution Accuracy metric for Text-to-SQL evaluation.
The metric evaluates queries by executing them against the database and comparing
the result sets, focusing on semantic correctness rather than syntactic form.

Following the Spider benchmark standards:
- Queries are executed against the actual database
- Result sets are compared for exact matches
- Type normalization is applied for fair comparison
- This is considered the most reliable indicator of user-facing accuracy
"""

import psycopg2
from typing import Dict, Any, List, Tuple, Optional, Union
from collections import Counter
from .base_metrics import BaseMetric, MetricResult, EvaluationContext


class ExecutionAccuracyMetric(BaseMetric):
    """
    Execution Accuracy (EX) Metric

    Evaluates SQL queries by executing them against the database and comparing
    result sets. This provides the most reliable measure of semantic correctness
    since it focuses on whether the query produces the intended results.

    Features:
    - Database execution with timeout protection
    - Result set normalization and comparison
    - Type-aware comparison (handling numeric precision, etc.)
    - Comprehensive error handling and reporting
    - Support for various result set sizes
    """

    def __init__(self, execution_timeout: int = 30):
        super().__init__("Execution Accuracy (EX)")
        self.execution_timeout = execution_timeout

    def evaluate(self, context: EvaluationContext) -> MetricResult:
        """
        Evaluate execution accuracy between ground truth and predicted SQL

        Args:
            context: Evaluation context containing SQL queries and database connection

        Returns:
            MetricResult with execution accuracy score
        """
        return self._safe_evaluate(context, self._evaluate_execution)

    def _evaluate_execution(self, context: EvaluationContext) -> MetricResult:
        """Internal execution accuracy evaluation"""

        # Validate inputs
        if not context.ground_truth_sql or not context.predicted_sql:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={
                    'reason': 'Empty or missing SQL query',
                    'ground_truth_executed': False,
                    'predicted_executed': False
                }
            )

        if not context.database_connection:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={'reason': 'No database connection provided'},
                error_message="Database connection required for execution accuracy"
            )

        # Execute ground truth query
        gt_result, gt_error = self._execute_query(
            context.ground_truth_sql,
            context.database_connection
        )

        # Execute predicted query
        pred_result, pred_error = self._execute_query(
            context.predicted_sql,
            context.database_connection
        )

        # Handle execution errors
        if gt_error:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={
                    'reason': 'Ground truth query execution failed',
                    'ground_truth_error': gt_error,
                    'predicted_executed': pred_error is None
                },
                error_message=f"Ground truth query failed: {gt_error}"
            )

        if pred_error:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={
                    'reason': 'Predicted query execution failed',
                    'predicted_error': pred_error,
                    'ground_truth_executed': True,
                    'ground_truth_rows': len(gt_result) if gt_result else 0
                },
                error_message=f"Predicted query failed: {pred_error}"
            )

        # Compare result sets
        results_match, comparison_details = self._compare_results(gt_result, pred_result)

        # Prepare comprehensive details
        details = {
            'ground_truth_executed': True,
            'predicted_executed': True,
            'ground_truth_rows': len(gt_result) if gt_result else 0,
            'predicted_rows': len(pred_result) if pred_result else 0,
            'results_match': results_match,
            'comparison_details': comparison_details
        }

        return self._create_result(
            score=1.0 if results_match else 0.0,
            is_correct=results_match,
            details=details
        )

    def _execute_query(self, sql_query: str, db_connection) -> Tuple[Optional[List], Optional[str]]:
        """
        Execute SQL query against database with error handling

        Args:
            sql_query: SQL query to execute
            db_connection: Database connection object

        Returns:
            Tuple of (results, error_message)
        """
        try:
            # Check if connection has execute_query method (our DatabaseConnection)
            if hasattr(db_connection, 'execute_query'):
                return db_connection.execute_query(sql_query)

            # Check if connection has get_raw_connection method
            elif hasattr(db_connection, 'get_raw_connection'):
                conn = db_connection.get_raw_connection()
                cursor = conn.cursor()

                # Set query timeout
                cursor.execute(f"SET statement_timeout = {self.execution_timeout * 1000}")

                # Execute the query
                cursor.execute(sql_query)

                # Fetch results
                try:
                    results = cursor.fetchall()
                    return results, None
                except Exception:
                    # Query doesn't return results (e.g., INSERT, UPDATE)
                    return [], None

            # Direct connection usage (fallback)
            else:
                cursor = db_connection.cursor()

                # Set query timeout
                cursor.execute(f"SET statement_timeout = {self.execution_timeout * 1000}")

                # Execute the query
                cursor.execute(sql_query)

                # Fetch results
                try:
                    results = cursor.fetchall()
                    return results, None
                except Exception:
                    # Query doesn't return results (e.g., INSERT, UPDATE)
                    return [], None

        except Exception as e:
            return None, f"Query execution error: {str(e)}"

    def _compare_results(self, gt_results: List, pred_results: List) -> Tuple[bool, Dict[str, Any]]:
        """
        Compare two result sets for equality

        Args:
            gt_results: Ground truth results
            pred_results: Predicted results

        Returns:
            Tuple of (match_boolean, comparison_details)
        """
        # Handle None/empty cases
        if gt_results is None:
            gt_results = []
        if pred_results is None:
            pred_results = []

        # Basic size comparison
        if len(gt_results) != len(pred_results):
            return False, {
                'size_mismatch': True,
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'size_difference': len(pred_results) - len(gt_results)
            }

        # Empty results are considered equal
        if len(gt_results) == 0:
            return True, {
                'both_empty': True,
                'gt_size': 0,
                'pred_size': 0
            }

        # Normalize and compare results
        gt_normalized = self._normalize_results(gt_results)
        pred_normalized = self._normalize_results(pred_results)

        # Use Counter for multiset comparison (handles row order differences)
        gt_counter = Counter(gt_normalized)
        pred_counter = Counter(pred_normalized)

        results_match = gt_counter == pred_counter

        # Detailed comparison
        comparison_details = {
            'gt_size': len(gt_results),
            'pred_size': len(pred_results),
            'normalized_match': results_match
        }

        if not results_match:
            # Analyze differences
            missing_rows = list((gt_counter - pred_counter).elements())
            extra_rows = list((pred_counter - gt_counter).elements())

            comparison_details.update({
                'missing_rows_count': len(missing_rows),
                'extra_rows_count': len(extra_rows),
                'missing_rows_sample': missing_rows[:5],  # Show first 5
                'extra_rows_sample': extra_rows[:5]       # Show first 5
            })

            # Calculate partial overlap
            intersection_size = sum((gt_counter & pred_counter).values())
            union_size = sum((gt_counter | pred_counter).values())
            overlap_ratio = intersection_size / union_size if union_size > 0 else 0.0

            comparison_details['overlap_ratio'] = overlap_ratio

        return results_match, comparison_details

    def _normalize_results(self, results: List) -> List[Tuple]:
        """
        Normalize result set for comparison

        Args:
            results: Raw database results

        Returns:
            List of normalized tuples
        """
        normalized = []

        for row in results:
            normalized_row = []
            for value in row:
                normalized_value = self._normalize_value(value)
                normalized_row.append(normalized_value)
            normalized.append(tuple(normalized_row))

        return normalized

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize a single value for comparison

        Args:
            value: Raw value from database

        Returns:
            Normalized value
        """
        if value is None:
            return None

        # Handle numeric types
        if isinstance(value, (int, float)):
            # For floating point numbers, round to avoid precision issues
            if isinstance(value, float):
                return round(value, 10)  # 10 decimal places should be sufficient
            return value

        # Handle string types
        if isinstance(value, str):
            return value.strip()

        # Handle boolean types
        if isinstance(value, bool):
            return value

        # Handle date/datetime types
        if hasattr(value, 'isoformat'):
            return value.isoformat()

        # For other types, convert to string
        return str(value)

    def get_description(self) -> str:
        """Get description of the metric"""
        return (
            "Execution Accuracy (EX) evaluates SQL queries by executing them "
            "against the database and comparing result sets. This focuses on "
            "semantic correctness and is widely regarded as the most reliable "
            "indicator of user-facing accuracy."
        )

    def get_scoring_info(self) -> Dict[str, Any]:
        """Get information about how scoring works"""
        return {
            "score_range": "0.0 to 1.0",
            "score_interpretation": {
                "1.0": "Result sets match exactly after normalization",
                "0.0": "Result sets differ or execution failed"
            },
            "features": [
                "Database execution with timeout protection",
                "Result set normalization (type-aware)",
                "Row order independence",
                "Comprehensive error handling",
                "Detailed mismatch analysis"
            ],
            "timeout_seconds": self.execution_timeout
        }


class ExecutionAccuracyWithRetry(ExecutionAccuracyMetric):
    """
    Enhanced Execution Accuracy metric with retry logic for transient failures
    """

    def __init__(self, execution_timeout: int = 30, max_retries: int = 3):
        super().__init__(execution_timeout)
        self.max_retries = max_retries
        self.name = "Execution Accuracy with Retry (EX+)"

    def _execute_query(self, sql_query: str, db_connection) -> Tuple[Optional[List], Optional[str]]:
        """Execute query with retry logic"""
        last_error = None

        for attempt in range(self.max_retries + 1):
            result, error = super()._execute_query(sql_query, db_connection)

            if error is None:
                return result, None

            last_error = error

            # Don't retry for syntax errors or similar non-transient issues
            if any(keyword in error.lower() for keyword in
                   ['syntax error', 'column', 'table', 'function', 'does not exist']):
                break

        return None, last_error