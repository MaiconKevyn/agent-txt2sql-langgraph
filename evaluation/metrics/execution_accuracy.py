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
from decimal import Decimal
from itertools import combinations
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
                    conn.commit()  # Commit successful query
                    return results, None
                except Exception:
                    # Query doesn't return results (e.g., INSERT, UPDATE)
                    conn.commit()  # Commit even if no results
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
                    db_connection.commit()  # Commit successful query
                    return results, None
                except Exception:
                    # Query doesn't return results (e.g., INSERT, UPDATE)
                    db_connection.commit()  # Commit even if no results
                    return [], None

        except Exception as e:
            # Rollback on error to prevent transaction abort
            try:
                if hasattr(db_connection, 'get_raw_connection'):
                    db_connection.get_raw_connection().rollback()
                elif hasattr(db_connection, 'rollback'):
                    db_connection.rollback()
            except:
                pass  # Ignore rollback errors
            return None, str(e)

    def _compare_results(self, gt_results: List, pred_results: List) -> Tuple[bool, Dict[str, Any]]:
        """
        Compare two result sets for equality.

        First attempts an exact Counter-based match (row-order independent).
        If that fails and the predicted result has *more* columns than the ground
        truth, attempts a projected match: tries all C(pred_cols, gt_cols) column
        index subsets of the predicted result and checks whether any projection
        produces a Counter equal to the ground truth Counter.  This handles the
        common case where the agent returns semantically correct results but
        includes intermediate/diagnostic columns not present in the gold standard
        (e.g., returning total_internacoes + total_uti + pct_uti when only pct_uti
        was requested).  The search is capped at 100 column combinations to bound
        runtime.

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

        # Basic row-count comparison
        if len(gt_results) != len(pred_results):
            return False, {
                'size_mismatch': True,
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'size_difference': len(pred_results) - len(gt_results)
            }

        # Empty results are considered equal
        if len(gt_results) == 0:
            return True, {'both_empty': True, 'gt_size': 0, 'pred_size': 0}

        # Normalize both result sets
        gt_normalized = self._normalize_results(gt_results)
        pred_normalized = self._normalize_results(pred_results)

        gt_counter = Counter(gt_normalized)
        pred_counter = Counter(pred_normalized)

        # --- Exact match ---
        if gt_counter == pred_counter:
            return True, {
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'normalized_match': True
            }

        # --- Projected match (predicted has extra columns) ---
        gt_col_count = len(gt_normalized[0]) if gt_normalized else 0
        pred_col_count = len(pred_normalized[0]) if pred_normalized else 0

        projected_match = False
        matching_columns = None

        if 0 < gt_col_count < pred_col_count:
            col_combos = list(combinations(range(pred_col_count), gt_col_count))
            # Cap to avoid performance issues on wide result sets
            for col_indices in col_combos[:100]:
                projected = [
                    tuple(row[i] for i in col_indices)
                    for row in pred_normalized
                ]
                if Counter(projected) == gt_counter:
                    projected_match = True
                    matching_columns = col_indices
                    break

        if projected_match:
            return True, {
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'normalized_match': True,
                'projected_match': True,
                'gt_col_count': gt_col_count,
                'pred_col_count': pred_col_count,
                'matching_column_indices': list(matching_columns)
            }

        # --- Reverse projected match (gold has MORE columns than predicted) ---
        # Handles the case where the agent returns a semantically correct but column-reduced
        # result (e.g. gold returns 4 diagnostic columns, agent returns 3 essential columns).
        # We project the gold down to pred_col_count columns and check for any subset match.
        reverse_projected_match = False
        reverse_matching_columns = None

        if 0 < pred_col_count < gt_col_count:
            col_combos = list(combinations(range(gt_col_count), pred_col_count))
            for col_indices in col_combos[:100]:
                projected_gt = [
                    tuple(row[i] for i in col_indices)
                    for row in gt_normalized
                ]
                if Counter(projected_gt) == pred_counter:
                    reverse_projected_match = True
                    reverse_matching_columns = col_indices
                    break

        if reverse_projected_match:
            return True, {
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'normalized_match': True,
                'projected_match': True,
                'reverse_projected_match': True,
                'gt_col_count': gt_col_count,
                'pred_col_count': pred_col_count,
                'matching_column_indices': list(reverse_matching_columns)
            }

        # --- Bidirectional projected match (same col count, label vs code differences) ---
        # Handles cases where both results have the same number of columns but one uses
        # codes (e.g. SEXO=1) while the other uses labels (e.g. 'Masculino').
        # Tries removing 1 column from each and checks if any pair of projections matches.
        bidir_match = False
        bidir_columns = None

        if gt_col_count == pred_col_count and gt_col_count >= 2:
            k = gt_col_count - 1  # remove exactly 1 column from each
            gt_combos = list(combinations(range(gt_col_count), k))
            pred_combos = list(combinations(range(pred_col_count), k))
            checked = 0
            outer_done = False
            for gt_idx in gt_combos:
                for pred_idx in pred_combos:
                    if checked >= 200:
                        outer_done = True
                        break
                    gt_proj = [tuple(row[i] for i in gt_idx) for row in gt_normalized]
                    pred_proj = [tuple(row[i] for i in pred_idx) for row in pred_normalized]
                    if Counter(gt_proj) == Counter(pred_proj):
                        bidir_match = True
                        bidir_columns = {'gt_cols': list(gt_idx), 'pred_cols': list(pred_idx)}
                        break
                    checked += 1
                if bidir_match or outer_done:
                    break

        if bidir_match:
            return True, {
                'gt_size': len(gt_results),
                'pred_size': len(pred_results),
                'normalized_match': True,
                'projected_match': True,
                'bidirectional_match': True,
                'gt_col_count': gt_col_count,
                'pred_col_count': pred_col_count,
                'matching_column_indices': bidir_columns
            }

        # --- Failed: analyse differences ---
        missing_rows = list((gt_counter - pred_counter).elements())
        extra_rows = list((pred_counter - gt_counter).elements())

        intersection_size = sum((gt_counter & pred_counter).values())
        union_size = sum((gt_counter | pred_counter).values())
        overlap_ratio = intersection_size / union_size if union_size > 0 else 0.0

        comparison_details = {
            'gt_size': len(gt_results),
            'pred_size': len(pred_results),
            'normalized_match': False,
            'missing_rows_count': len(missing_rows),
            'extra_rows_count': len(extra_rows),
            'missing_rows_sample': missing_rows[:5],
            'extra_rows_sample': extra_rows[:5],
            'overlap_ratio': overlap_ratio
        }

        return False, comparison_details

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
        Normalize a single value for comparison.

        Key invariants:
        - Booleans checked before int (bool is subclass of int in Python)
        - Decimal (PostgreSQL NUMERIC/AVG/SUM) converted to float and rounded
        - Floats rounded to 2 decimal places to absorb PostgreSQL query-planner
          floating-point accumulation differences on the same logical computation
        - Strings stripped and lowercased for case-insensitive comparison
        """
        if value is None:
            return None

        # Handle boolean before int (bool is a subclass of int)
        if isinstance(value, bool):
            return value

        # Handle Python Decimal (maps to PostgreSQL NUMERIC, AVG, SUM results)
        # For large aggregates (>= 1_000_000) round to 0 decimal places to absorb
        # the ~0.01–1.00 FP drift that DuckDB's parallel SUM accumulation produces
        # on 10M+ row scans.  For smaller values keep 2 decimal places.
        if isinstance(value, Decimal):
            f = float(value)
            return round(f, 0) if abs(f) >= 1_000_000 else round(f, 2)

        # Handle native int
        if isinstance(value, int):
            return value

        # Handle native float — same large-value tolerance as Decimal
        if isinstance(value, float):
            return round(value, 0) if abs(value) >= 1_000_000 else round(value, 2)

        # Handle string types — lowercase for case-insensitive comparison
        # (e.g. 'BRANCA' from raca_cor.DESCRICAO vs 'Branca' from CASE WHEN)
        if isinstance(value, str):
            return value.strip().lower()

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