"""
Component Matching (CM) Metric Implementation

This module implements the Component Matching metric for Text-to-SQL evaluation.
The metric evaluates each SQL clause (SELECT, FROM, WHERE, etc.) independently,
allowing for flexibility in clause ordering while ensuring that essential parts
of the query are preserved.

Following the Spider benchmark standards:
- Each clause is evaluated independently
- Clause reordering is allowed
- Equivalent expressions within clauses are considered
- Partial credit is given based on matching components
"""

from typing import Dict, Any, List, Set
import re
from .base_metrics import BaseMetric, MetricResult, EvaluationContext, SQLParser, SQLNormalizer
from .improved_sql_parser import ImprovedSQLParser, ImprovedColumnComparator


class ComponentMatchingMetric(BaseMetric):
    """
    Component Matching (CM) Metric

    Evaluates SQL queries by comparing individual components (clauses) independently.
    This allows for flexibility in clause ordering and provides more granular
    feedback on which parts of the query are correct.

    Components evaluated:
    - SELECT clause (columns, functions, aliases)
    - FROM clause (tables, subqueries)
    - WHERE clause (conditions, operators)
    - GROUP BY clause
    - HAVING clause
    - ORDER BY clause
    - LIMIT/OFFSET clause
    - JOIN operations
    """

    def __init__(self):
        super().__init__("Component Matching (CM)")
        self.clause_weights = {
            'select': 0.25,    # Most important
            'from': 0.20,      # Table selection critical
            'where': 0.20,     # Filtering conditions
            'joins': 0.15,     # Join operations
            'group_by': 0.10,  # Aggregation grouping
            'order_by': 0.05,  # Result ordering
            'having': 0.03,    # Post-aggregation filtering
            'limit': 0.02      # Result limiting
        }

    def evaluate(self, context: EvaluationContext) -> MetricResult:
        """
        Evaluate component matching between ground truth and predicted SQL

        Args:
            context: Evaluation context containing SQL queries

        Returns:
            MetricResult with component-wise scores
        """
        return self._safe_evaluate(context, self._evaluate_components)

    def _evaluate_components(self, context: EvaluationContext) -> MetricResult:
        """Internal component matching evaluation"""

        # Validate inputs
        if not context.ground_truth_sql or not context.predicted_sql:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={
                    'reason': 'Empty or missing SQL query',
                    'component_scores': {}
                }
            )

        # Extract components from both queries using improved parser
        gt_components = ImprovedSQLParser.extract_components(context.ground_truth_sql)
        pred_components = ImprovedSQLParser.extract_components(context.predicted_sql)

        # Evaluate each component
        component_scores = {}
        component_details = {}

        for component_name in self.clause_weights.keys():
            score, details = self._evaluate_component(
                component_name,
                gt_components.get(component_name, ''),
                pred_components.get(component_name, '')
            )
            component_scores[component_name] = score
            component_details[component_name] = details

        # Calculate weighted overall score
        overall_score = sum(
            component_scores[comp] * weight
            for comp, weight in self.clause_weights.items()
        )

        # Determine if the query is considered correct (threshold: 0.8)
        is_correct = overall_score >= 0.8

        details = {
            'overall_score': overall_score,
            'component_scores': component_scores,
            'component_details': component_details,
            'ground_truth_components': gt_components,
            'predicted_components': pred_components,
            'clause_weights': self.clause_weights
        }

        return self._create_result(
            score=overall_score,
            is_correct=is_correct,
            details=details
        )

    def _evaluate_component(self, component_name: str, gt_component: str, pred_component: str) -> tuple:
        """
        Evaluate a specific SQL component

        Args:
            component_name: Name of the component (e.g., 'select', 'where')
            gt_component: Ground truth component content
            pred_component: Predicted component content

        Returns:
            Tuple of (score, details)
        """
        if component_name == 'select':
            return self._evaluate_select_clause(gt_component, pred_component)
        elif component_name == 'from':
            return self._evaluate_from_clause(gt_component, pred_component)
        elif component_name == 'where':
            return self._evaluate_where_clause(gt_component, pred_component)
        elif component_name == 'joins':
            return self._evaluate_joins_clause(gt_component, pred_component)
        elif component_name in ['group_by', 'order_by', 'having']:
            return self._evaluate_generic_clause(gt_component, pred_component)
        elif component_name == 'limit':
            return self._evaluate_limit_clause(gt_component, pred_component)
        else:
            return self._evaluate_generic_clause(gt_component, pred_component)

    def _evaluate_select_clause(self, gt_select: str, pred_select: str) -> tuple:
        """Evaluate SELECT clause with improved column and function matching"""
        if not gt_select.strip() and not pred_select.strip():
            return 1.0, {'match': 'both_empty'}

        if not gt_select.strip() or not pred_select.strip():
            return 0.0, {'match': 'one_empty', 'gt_select': gt_select, 'pred_select': pred_select}

        # Use improved column comparator
        gt_columns = ImprovedColumnComparator.extract_select_items(gt_select)
        pred_columns = ImprovedColumnComparator.extract_select_items(pred_select)

        # Get detailed comparison with alias handling
        comparison = ImprovedColumnComparator.compare_select_items(gt_columns, pred_columns)

        return comparison['jaccard_similarity'], comparison

    def _evaluate_from_clause(self, gt_from: str, pred_from: str) -> tuple:
        """Evaluate FROM clause with table matching"""
        if not gt_from.strip() and not pred_from.strip():
            return 1.0, {'match': 'both_empty'}

        if not gt_from.strip() or not pred_from.strip():
            return 0.0, {'match': 'one_empty'}

        # Extract table names
        gt_tables = self._extract_table_names(gt_from)
        pred_tables = self._extract_table_names(pred_from)

        # Calculate overlap
        gt_set = set(gt_tables)
        pred_set = set(pred_tables)

        if not gt_set and not pred_set:
            return 1.0, {'match': 'both_empty_after_parsing'}

        if not gt_set or not pred_set:
            return 0.0, {'match': 'one_empty_after_parsing'}

        # Calculate Jaccard similarity
        intersection = len(gt_set.intersection(pred_set))
        union = len(gt_set.union(pred_set))
        score = intersection / union if union > 0 else 0.0

        details = {
            'gt_tables': gt_tables,
            'pred_tables': pred_tables,
            'intersection_count': intersection,
            'union_count': union,
            'jaccard_similarity': score
        }

        return score, details

    def _evaluate_where_clause(self, gt_where: str, pred_where: str) -> tuple:
        """Evaluate WHERE clause with condition matching"""
        if not gt_where.strip() and not pred_where.strip():
            return 1.0, {'match': 'both_empty'}

        if not gt_where.strip() or not pred_where.strip():
            return 0.0, {'match': 'one_empty'}

        # Extract conditions
        gt_conditions = self._extract_conditions(gt_where)
        pred_conditions = self._extract_conditions(pred_where)

        # Calculate overlap
        gt_set = set(gt_conditions)
        pred_set = set(pred_conditions)

        if not gt_set and not pred_set:
            return 1.0, {'match': 'both_empty_after_parsing'}

        if not gt_set or not pred_set:
            return 0.0, {'match': 'one_empty_after_parsing'}

        # Calculate Jaccard similarity
        intersection = len(gt_set.intersection(pred_set))
        union = len(gt_set.union(pred_set))
        score = intersection / union if union > 0 else 0.0

        details = {
            'gt_conditions': gt_conditions,
            'pred_conditions': pred_conditions,
            'intersection_count': intersection,
            'union_count': union,
            'jaccard_similarity': score
        }

        return score, details

    def _evaluate_joins_clause(self, gt_joins: str, pred_joins: str) -> tuple:
        """Evaluate JOIN operations"""
        return self._evaluate_generic_clause(gt_joins, pred_joins)

    def _evaluate_generic_clause(self, gt_clause: str, pred_clause: str) -> tuple:
        """Generic clause evaluation using normalized string comparison"""
        if not gt_clause.strip() and not pred_clause.strip():
            return 1.0, {'match': 'both_empty'}

        if not gt_clause.strip() or not pred_clause.strip():
            return 0.0, {'match': 'one_empty'}

        # Normalize and compare
        gt_normalized = SQLNormalizer.normalize_sql(gt_clause)
        pred_normalized = SQLNormalizer.normalize_sql(pred_clause)

        score = 1.0 if gt_normalized == pred_normalized else 0.0

        details = {
            'gt_normalized': gt_normalized,
            'pred_normalized': pred_normalized,
            'exact_match': score == 1.0
        }

        return score, details

    def _evaluate_limit_clause(self, gt_limit: str, pred_limit: str) -> tuple:
        """Evaluate LIMIT clause with number extraction"""
        if not gt_limit.strip() and not pred_limit.strip():
            return 1.0, {'match': 'both_empty'}

        if not gt_limit.strip() or not pred_limit.strip():
            return 0.0, {'match': 'one_empty'}

        # Extract numbers from LIMIT clause
        gt_numbers = re.findall(r'\d+', gt_limit)
        pred_numbers = re.findall(r'\d+', pred_limit)

        score = 1.0 if gt_numbers == pred_numbers else 0.0

        details = {
            'gt_numbers': gt_numbers,
            'pred_numbers': pred_numbers,
            'match': score == 1.0
        }

        return score, details


    def _extract_table_names(self, from_clause: str) -> List[str]:
        """Extract table names from FROM clause"""
        if not from_clause.strip():
            return []

        # Remove FROM keyword if present
        clause = re.sub(r'^\s*FROM\s+', '', from_clause.strip(), flags=re.IGNORECASE)

        # Simple extraction - split by comma and clean
        tables = []
        for table in clause.split(','):
            table = table.strip()
            if table:
                # Remove aliases (AS keyword or space-separated)
                table = re.sub(r'\s+(AS\s+)?\w+\s*$', '', table, flags=re.IGNORECASE)
                table = table.strip()
                # Remove quotes if present
                table = table.strip('"\'`')
                tables.append(table)

        return tables

    def _extract_conditions(self, where_clause: str) -> List[str]:
        """Extract individual conditions from WHERE clause"""
        if not where_clause.strip():
            return []

        # Remove WHERE keyword if present
        clause = re.sub(r'^\s*WHERE\s+', '', where_clause.strip(), flags=re.IGNORECASE)

        # Split by AND/OR and normalize
        # This is a simplified approach - more sophisticated parsing could be added
        conditions = []
        parts = re.split(r'\s+(AND|OR)\s+', clause, flags=re.IGNORECASE)

        for part in parts:
            if part.upper() not in ('AND', 'OR'):
                normalized = SQLNormalizer.normalize_sql(part.strip())
                if normalized:
                    conditions.append(normalized)

        return conditions

    def get_description(self) -> str:
        """Get description of the metric"""
        return (
            "Component Matching (CM) evaluates SQL queries by comparing individual "
            "components (clauses) independently. This allows for flexibility in "
            "clause ordering while ensuring essential parts are preserved."
        )

    def get_scoring_info(self) -> Dict[str, Any]:
        """Get information about how scoring works"""
        return {
            "score_range": "0.0 to 1.0",
            "score_interpretation": {
                "1.0": "All components match perfectly",
                "0.8+": "Query considered correct (high threshold)",
                "0.0": "No components match"
            },
            "component_weights": self.clause_weights,
            "components_evaluated": list(self.clause_weights.keys())
        }