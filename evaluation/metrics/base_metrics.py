"""
Base classes and utilities for Text-to-SQL evaluation metrics.

This module provides the foundational infrastructure for implementing
evaluation metrics following Spider benchmark standards.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import re
import sqlparse
from sqlparse import sql, tokens


@dataclass
class MetricResult:
    """Result of a metric evaluation"""
    metric_name: str
    score: float  # 0.0 to 1.0
    is_correct: bool
    details: Dict[str, Any]
    error_message: Optional[str] = None


@dataclass
class EvaluationContext:
    """Context information for evaluation"""
    question_id: str
    question: str
    ground_truth_sql: str
    predicted_sql: str
    database_connection: Optional[Any] = None
    tables_info: Optional[Dict[str, Any]] = None


class SQLNormalizer:
    """Utility class for normalizing SQL queries for comparison"""

    @staticmethod
    def normalize_sql(sql_query: str) -> str:
        """
        Normalize SQL query for comparison by:
        - Removing comments
        - Standardizing whitespace
        - Converting to lowercase keywords
        - Removing trailing semicolons
        """
        if not sql_query or not sql_query.strip():
            return ""

        # Parse the SQL
        try:
            parsed = sqlparse.parse(sql_query)[0]
            return SQLNormalizer._normalize_statement(parsed)
        except Exception:
            # Fallback to basic normalization if parsing fails
            return SQLNormalizer._basic_normalize(sql_query)

    @staticmethod
    def _normalize_statement(statement: sql.Statement) -> str:
        """Normalize a parsed SQL statement"""
        normalized_tokens = []

        for token in statement.tokens:
            if token.ttype in (tokens.Comment.Single, tokens.Comment.Multiline):
                continue
            if token.ttype in tokens.Whitespace:
                if normalized_tokens and normalized_tokens[-1] != ' ':
                    normalized_tokens.append(' ')
                continue

            if hasattr(token, 'tokens'):  # Token group
                normalized_tokens.append(SQLNormalizer._normalize_token_group(token))
            else:
                normalized_tokens.append(SQLNormalizer._normalize_token(token))

        # Join and clean up
        result = ''.join(normalized_tokens).strip()
        result = re.sub(r'\s+', ' ', result)  # Normalize whitespace
        result = result.rstrip(';')  # Remove trailing semicolon
        return result

    @staticmethod
    def _normalize_token_group(token_group) -> str:
        """Normalize a group of tokens"""
        normalized = []
        for token in token_group.tokens:
            if token.ttype in (tokens.Comment.Single, tokens.Comment.Multiline):
                continue
            if token.ttype in tokens.Whitespace:
                if normalized and normalized[-1] != ' ':
                    normalized.append(' ')
                continue

            if hasattr(token, 'tokens'):
                normalized.append(SQLNormalizer._normalize_token_group(token))
            else:
                normalized.append(SQLNormalizer._normalize_token(token))

        return ''.join(normalized)

    @staticmethod
    def _normalize_token(token) -> str:
        """Normalize a single token"""
        if token.ttype in tokens.Keyword:
            return token.value.upper()
        elif token.ttype in tokens.Name:
            # Preserve case for identifiers but normalize quotes
            value = token.value
            if value.startswith('"') and value.endswith('"'):
                return f'"{value[1:-1]}"'
            return value
        else:
            return token.value

    @staticmethod
    def _basic_normalize(sql_query: str) -> str:
        """Basic normalization fallback when parsing fails"""
        # Remove comments
        sql_query = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
        sql_query = re.sub(r'/\*.*?\*/', '', sql_query, flags=re.DOTALL)

        # Normalize whitespace
        sql_query = re.sub(r'\s+', ' ', sql_query.strip())

        # Remove trailing semicolon
        sql_query = sql_query.rstrip(';')

        return sql_query


class SQLParser:
    """Utility class for parsing SQL queries into components"""

    @staticmethod
    def extract_components(sql_query: str) -> Dict[str, Any]:
        """
        Extract SQL components (SELECT, FROM, WHERE, etc.)
        Returns dict with component names as keys and parsed content as values
        """
        try:
            parsed = sqlparse.parse(sql_query)[0]
            components = {
                'select': [],
                'from': [],
                'where': [],
                'group_by': [],
                'having': [],
                'order_by': [],
                'limit': [],
                'joins': []
            }

            current_component = None
            for token in parsed.tokens:
                if token.ttype in tokens.Whitespace:
                    continue

                token_value = token.value.upper()

                if token.ttype in tokens.Keyword:
                    if token_value == 'SELECT':
                        current_component = 'select'
                    elif token_value == 'FROM':
                        current_component = 'from'
                    elif token_value == 'WHERE':
                        current_component = 'where'
                    elif token_value in ('GROUP', 'GROUP BY'):
                        current_component = 'group_by'
                    elif token_value == 'HAVING':
                        current_component = 'having'
                    elif token_value in ('ORDER', 'ORDER BY'):
                        current_component = 'order_by'
                    elif token_value == 'LIMIT':
                        current_component = 'limit'
                    elif token_value in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN'):
                        current_component = 'joins'
                    continue

                if current_component and current_component in components:
                    components[current_component].append(token.value)

            # Clean up components
            for key, value in components.items():
                if value:
                    components[key] = ' '.join(str(v) for v in value).strip()
                else:
                    components[key] = ''

            return components

        except Exception as e:
            # Return empty components if parsing fails
            return {
                'select': '',
                'from': '',
                'where': '',
                'group_by': '',
                'having': '',
                'order_by': '',
                'limit': '',
                'joins': ''
            }


class BaseMetric(ABC):
    """Abstract base class for evaluation metrics"""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def evaluate(self, context: EvaluationContext) -> MetricResult:
        """
        Evaluate the metric for given context

        Args:
            context: Evaluation context with ground truth and predicted SQL

        Returns:
            MetricResult with score and details
        """
        pass

    def _create_result(self, score: float, is_correct: bool, details: Dict[str, Any],
                      error_message: Optional[str] = None) -> MetricResult:
        """Helper method to create MetricResult"""
        return MetricResult(
            metric_name=self.name,
            score=score,
            is_correct=is_correct,
            details=details,
            error_message=error_message
        )

    def _safe_evaluate(self, context: EvaluationContext, evaluation_func) -> MetricResult:
        """
        Safely execute evaluation function with error handling

        Args:
            context: Evaluation context
            evaluation_func: Function to execute for evaluation

        Returns:
            MetricResult with error handling
        """
        try:
            return evaluation_func(context)
        except Exception as e:
            return self._create_result(
                score=0.0,
                is_correct=False,
                details={'error': str(e)},
                error_message=f"Error in {self.name}: {str(e)}"
            )