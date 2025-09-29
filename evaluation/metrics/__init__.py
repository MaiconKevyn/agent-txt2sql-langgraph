"""
Text-to-SQL Evaluation Metrics

This package provides comprehensive evaluation metrics for Text-to-SQL systems
following the standards established in the Spider benchmark and related work.

Available metrics:
- Exact Match (EM): Syntactic matching of SQL queries
- Component Matching (CM): Clause-level evaluation of SQL components
- Execution Accuracy (EX): Semantic evaluation through result set comparison
"""

from .base_metrics import BaseMetric, MetricResult, EvaluationContext
from .exact_match import ExactMatchMetric
from .component_matching import ComponentMatchingMetric
from .execution_accuracy import ExecutionAccuracyMetric

__all__ = [
    'BaseMetric',
    'MetricResult',
    'EvaluationContext',
    'ExactMatchMetric',
    'ComponentMatchingMetric',
    'ExecutionAccuracyMetric'
]