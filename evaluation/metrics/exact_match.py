"""
Exact Match (EM) Metric Implementation

This module implements the Exact Match metric for Text-to-SQL evaluation.
The metric compares the syntactic structure of SQL queries after normalization,
ensuring that the generated query matches the ground truth exactly in structure
and content.

Following the Spider benchmark standards:
- Queries must match exactly after normalization
- Whitespace, comments, and minor formatting differences are ignored
- Case sensitivity is handled appropriately for keywords vs identifiers
- Clause ordering must be identical
"""

from typing import Dict, Any
from .base_metrics import BaseMetric, MetricResult, EvaluationContext, SQLNormalizer


class ExactMatchMetric(BaseMetric):
    """
    Exact Match (EM) Metric

    Evaluates whether the predicted SQL query matches the ground truth exactly
    after normalization. This is the strictest metric, requiring perfect
    syntactic correspondence.

    Normalization includes:
    - Keyword case standardization (uppercase)
    - Whitespace normalization
    - Comment removal
    - Trailing semicolon removal
    - Quote normalization for identifiers
    """

    def __init__(self):
        super().__init__("Exact Match (EM)")

    def evaluate(self, context: EvaluationContext) -> MetricResult:
        """
        Evaluate exact match between ground truth and predicted SQL

        Args:
            context: Evaluation context containing SQL queries

        Returns:
            MetricResult with exact match score (1.0 or 0.0)
        """
        return self._safe_evaluate(context, self._evaluate_exact_match)

    def _evaluate_exact_match(self, context: EvaluationContext) -> MetricResult:
        """Internal exact match evaluation"""

        # Validate inputs
        if not context.ground_truth_sql or not context.predicted_sql:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={
                    'ground_truth_normalized': '',
                    'predicted_normalized': '',
                    'reason': 'Empty or missing SQL query'
                }
            )

        # Normalize both queries
        gt_normalized = SQLNormalizer.normalize_sql(context.ground_truth_sql)
        pred_normalized = SQLNormalizer.normalize_sql(context.predicted_sql)

        # Check for exact match
        is_exact_match = gt_normalized == pred_normalized

        # Prepare detailed comparison
        details = {
            'ground_truth_original': context.ground_truth_sql,
            'predicted_original': context.predicted_sql,
            'ground_truth_normalized': gt_normalized,
            'predicted_normalized': pred_normalized,
            'match': is_exact_match
        }

        # Add difference analysis if not matching
        if not is_exact_match:
            details.update(self._analyze_differences(gt_normalized, pred_normalized))

        return self._create_result(
            score=1.0 if is_exact_match else 0.0,
            is_correct=is_exact_match,
            details=details
        )

    def _analyze_differences(self, gt_normalized: str, pred_normalized: str) -> Dict[str, Any]:
        """
        Analyze differences between normalized queries to provide helpful feedback

        Args:
            gt_normalized: Normalized ground truth SQL
            pred_normalized: Normalized predicted SQL

        Returns:
            Dict with difference analysis
        """
        analysis = {}

        # Basic length comparison
        analysis['length_difference'] = len(pred_normalized) - len(gt_normalized)

        # Character-level differences
        if len(gt_normalized) > 0 and len(pred_normalized) > 0:
            # Find first differing position
            min_len = min(len(gt_normalized), len(pred_normalized))
            first_diff = None
            for i in range(min_len):
                if gt_normalized[i] != pred_normalized[i]:
                    first_diff = i
                    break

            if first_diff is not None:
                analysis['first_difference_position'] = first_diff
                analysis['first_difference_context'] = {
                    'ground_truth': gt_normalized[max(0, first_diff-10):first_diff+10],
                    'predicted': pred_normalized[max(0, first_diff-10):first_diff+10]
                }

        # Word-level comparison
        gt_words = gt_normalized.split()
        pred_words = pred_normalized.split()

        analysis['word_count_difference'] = len(pred_words) - len(gt_words)

        # Find differing words
        max_words = max(len(gt_words), len(pred_words))
        differing_positions = []
        for i in range(max_words):
            gt_word = gt_words[i] if i < len(gt_words) else "<MISSING>"
            pred_word = pred_words[i] if i < len(pred_words) else "<EXTRA>"
            if gt_word != pred_word:
                differing_positions.append({
                    'position': i,
                    'ground_truth': gt_word,
                    'predicted': pred_word
                })

        analysis['differing_words'] = differing_positions[:5]  # Limit to first 5 differences

        # Common error patterns
        analysis['common_errors'] = self._detect_common_errors(gt_normalized, pred_normalized)

        return analysis

    def _detect_common_errors(self, gt_normalized: str, pred_normalized: str) -> list:
        """
        Detect common types of errors in SQL generation

        Args:
            gt_normalized: Normalized ground truth SQL
            pred_normalized: Normalized predicted SQL

        Returns:
            List of detected error patterns
        """
        errors = []

        # Check for missing/extra clauses
        gt_upper = gt_normalized.upper()
        pred_upper = pred_normalized.upper()

        clauses = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT']
        for clause in clauses:
            gt_has = clause in gt_upper
            pred_has = clause in pred_upper

            if gt_has and not pred_has:
                errors.append(f"Missing {clause} clause")
            elif not gt_has and pred_has:
                errors.append(f"Extra {clause} clause")

        # Check for quote-related issues
        if gt_normalized.count('"') != pred_normalized.count('"'):
            errors.append("Quote mismatch (identifier quoting)")

        # Check for aggregate function issues
        gt_agg = len([word for word in gt_normalized.upper().split()
                     if word in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']])
        pred_agg = len([word for word in pred_normalized.upper().split()
                       if word in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']])

        if gt_agg != pred_agg:
            errors.append("Aggregate function count mismatch")

        # Check for JOIN-related issues
        if 'JOIN' in gt_upper and 'JOIN' not in pred_upper:
            errors.append("Missing JOIN operation")
        elif 'JOIN' not in gt_upper and 'JOIN' in pred_upper:
            errors.append("Unexpected JOIN operation")

        return errors

    def get_description(self) -> str:
        """Get description of the metric"""
        return (
            "Exact Match (EM) evaluates whether the predicted SQL query matches "
            "the ground truth exactly after normalization. This metric requires "
            "perfect syntactic correspondence and is the strictest evaluation criterion."
        )

    def get_scoring_info(self) -> Dict[str, Any]:
        """Get information about how scoring works"""
        return {
            "score_range": "0.0 to 1.0",
            "score_interpretation": {
                "1.0": "Perfect exact match after normalization",
                "0.0": "No exact match"
            },
            "normalization_steps": [
                "Remove comments",
                "Normalize whitespace",
                "Standardize keyword case (uppercase)",
                "Remove trailing semicolons",
                "Normalize identifier quotes"
            ]
        }