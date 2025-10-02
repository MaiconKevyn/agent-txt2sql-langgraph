"""
Improved SQL Parser for Text-to-SQL Evaluation

This module provides an enhanced SQL parser that correctly handles:
1. WHERE clause extraction (not mixing with FROM)
2. Alias-aware column comparison
3. Better component parsing
"""

import re
import sqlparse
from sqlparse import sql, tokens
from typing import Dict, List, Any


class ImprovedSQLParser:
    """Enhanced SQL parser with better component extraction"""

    @staticmethod
    def extract_components(sql_query: str) -> Dict[str, str]:
        """
        Extract SQL components with improved parsing logic

        Args:
            sql_query: SQL query to parse

        Returns:
            Dict with component names as keys and content as values
        """
        if not sql_query or not sql_query.strip():
            return ImprovedSQLParser._empty_components()

        try:
            # First try advanced parsing
            components = ImprovedSQLParser._advanced_component_extraction(sql_query)

            # Validate that we got meaningful results
            if components['select'] or components['from'] or components['where']:
                return components
            else:
                # Fallback to regex-based parsing
                return ImprovedSQLParser._regex_component_extraction(sql_query)

        except Exception:
            # Final fallback
            return ImprovedSQLParser._regex_component_extraction(sql_query)

    @staticmethod
    def _advanced_component_extraction(sql_query: str) -> Dict[str, str]:
        """Advanced component extraction using sqlparse with better logic"""
        parsed = sqlparse.parse(sql_query)[0]
        components = ImprovedSQLParser._empty_components()

        # Track state
        current_component = None
        paren_depth = 0

        for token in parsed.flatten():
            if token.ttype in tokens.Whitespace:
                continue

            # Track parentheses depth
            if token.value == '(':
                paren_depth += 1
            elif token.value == ')':
                paren_depth -= 1

            # Only process top-level keywords (not in subqueries)
            if paren_depth == 0 and token.ttype in tokens.Keyword:
                token_upper = token.value.upper()

                if token_upper == 'SELECT':
                    current_component = 'select'
                elif token_upper == 'FROM':
                    current_component = 'from'
                elif token_upper == 'WHERE':
                    current_component = 'where'
                elif token_upper in ('GROUP', 'GROUP BY'):
                    current_component = 'group_by'
                elif token_upper == 'HAVING':
                    current_component = 'having'
                elif token_upper in ('ORDER', 'ORDER BY'):
                    current_component = 'order_by'
                elif token_upper == 'LIMIT':
                    current_component = 'limit'
                elif token_upper in ('JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'):
                    current_component = 'joins'
                else:
                    # Don't change component for other keywords
                    pass
                continue

            # Add content to current component
            if current_component and current_component in components:
                if components[current_component]:
                    components[current_component] += ' ' + token.value
                else:
                    components[current_component] = token.value

        # Clean up components
        for key in components:
            components[key] = components[key].strip()

        return components

    @staticmethod
    def _regex_component_extraction(sql_query: str) -> Dict[str, str]:
        """Fallback regex-based component extraction"""
        components = ImprovedSQLParser._empty_components()

        # Normalize query
        query = re.sub(r'\s+', ' ', sql_query.strip())
        query = query.rstrip(';')

        # Extract SELECT
        select_match = re.search(r'\bSELECT\s+(.*?)\s+FROM\b', query, re.IGNORECASE | re.DOTALL)
        if select_match:
            components['select'] = select_match.group(1).strip()

        # Extract FROM (up to WHERE, GROUP BY, ORDER BY, or end)
        from_match = re.search(r'\bFROM\s+(.*?)(?:\s+WHERE|\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)',
                              query, re.IGNORECASE | re.DOTALL)
        if from_match:
            components['from'] = from_match.group(1).strip()

        # Extract WHERE (up to GROUP BY, ORDER BY, or end)
        where_match = re.search(r'\bWHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)',
                               query, re.IGNORECASE | re.DOTALL)
        if where_match:
            components['where'] = where_match.group(1).strip()

        # Extract GROUP BY
        group_match = re.search(r'\bGROUP\s+BY\s+(.*?)(?:\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|$)',
                               query, re.IGNORECASE | re.DOTALL)
        if group_match:
            components['group_by'] = group_match.group(1).strip()

        # Extract HAVING
        having_match = re.search(r'\bHAVING\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|$)',
                                query, re.IGNORECASE | re.DOTALL)
        if having_match:
            components['having'] = having_match.group(1).strip()

        # Extract ORDER BY
        order_match = re.search(r'\bORDER\s+BY\s+(.*?)(?:\s+LIMIT|$)',
                               query, re.IGNORECASE | re.DOTALL)
        if order_match:
            components['order_by'] = order_match.group(1).strip()

        # Extract LIMIT
        limit_match = re.search(r'\bLIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            components['limit'] = limit_match.group(1).strip()

        return components

    @staticmethod
    def _empty_components() -> Dict[str, str]:
        """Return empty components dict"""
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


class ImprovedColumnComparator:
    """Enhanced column comparison that handles aliases better"""

    @staticmethod
    def extract_select_items(select_clause: str) -> List[str]:
        """
        Extract and normalize SELECT items with alias-aware comparison

        Args:
            select_clause: SELECT clause content

        Returns:
            List of normalized select items
        """
        if not select_clause.strip():
            return []

        # Remove SELECT keyword if present
        clause = re.sub(r'^\s*SELECT\s+', '', select_clause.strip(), flags=re.IGNORECASE)

        # Split by comma and process each item
        items = []
        for item in clause.split(','):
            item = item.strip()
            if item:
                normalized_item = ImprovedColumnComparator._normalize_select_item(item)
                items.append(normalized_item)

        return items

    @staticmethod
    def _normalize_select_item(item: str) -> str:
        """
        Normalize a single SELECT item with alias handling

        This function extracts the core expression and normalizes aliases
        """
        item = item.strip()

        # Check for alias pattern: "expression AS alias" or "expression alias"
        # Pattern 1: explicit AS
        as_match = re.match(r'^(.+?)\s+AS\s+(.+)$', item, re.IGNORECASE)
        if as_match:
            expression = as_match.group(1).strip()
            alias = as_match.group(2).strip()
            return ImprovedColumnComparator._create_normalized_expression(expression, alias)

        # Pattern 2: implicit alias (space-separated, no AS)
        # Look for patterns like "COUNT(*) total" or "name alias"
        # Split and check if last part could be an alias
        words = item.split()
        if len(words) >= 2:
            potential_alias = words[-1]

            # More robust alias detection
            # Check if it's NOT a SQL keyword, function, or operator
            if (re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', potential_alias) and  # Valid identifier
                potential_alias.upper() not in [
                    'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'AND', 'OR', 'NOT',
                    'IN', 'EXISTS', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'TRUE', 'FALSE',
                    'ASC', 'DESC', 'DISTINCT', 'ALL', 'ANY', 'SOME'
                ] and
                not re.match(r'.+\([^)]*\)$', potential_alias)):  # Not a function call

                expression = ' '.join(words[:-1])
                return ImprovedColumnComparator._create_normalized_expression(expression, potential_alias)

        # No alias detected, return the expression as-is
        return ImprovedColumnComparator._normalize_expression(item)

    @staticmethod
    def _create_normalized_expression(expression: str, alias: str) -> str:
        """
        Create normalized expression that allows alias flexibility

        For comparison purposes, we normalize the core expression and
        mark that it has an alias (but don't require exact alias match)
        """
        normalized_expr = ImprovedColumnComparator._normalize_expression(expression)

        # Create a pattern that represents "expression with some alias"
        # This allows different aliases to be considered equivalent
        return f"{normalized_expr} AS <alias>"

    @staticmethod
    def _normalize_expression(expression: str) -> str:
        """Normalize the core expression (without alias)"""
        expr = expression.strip()

        # Normalize function names to uppercase
        expr = re.sub(r'\b(count|sum|avg|min|max|extract)\b',
                     lambda m: m.group(1).upper(), expr, flags=re.IGNORECASE)

        # Normalize whitespace
        expr = re.sub(r'\s+', ' ', expr)

        return expr

    @staticmethod
    def compare_select_items(gt_items: List[str], pred_items: List[str]) -> Dict[str, Any]:
        """
        Compare SELECT items with improved alias handling

        Returns:
            Dict with comparison details
        """
        # Normalize both lists
        gt_normalized = [ImprovedColumnComparator._normalize_select_item(item) for item in gt_items]
        pred_normalized = [ImprovedColumnComparator._normalize_select_item(item) for item in pred_items]

        # Calculate similarity
        if not gt_normalized and not pred_normalized:
            return {
                'jaccard_similarity': 1.0,
                'gt_items': gt_items,
                'pred_items': pred_items,
                'gt_normalized': gt_normalized,
                'pred_normalized': pred_normalized,
                'exact_match': True
            }

        if not gt_normalized or not pred_normalized:
            return {
                'jaccard_similarity': 0.0,
                'gt_items': gt_items,
                'pred_items': pred_items,
                'gt_normalized': gt_normalized,
                'pred_normalized': pred_normalized,
                'exact_match': False
            }

        # Enhanced comparison with alias flexibility
        jaccard = ImprovedColumnComparator._calculate_flexible_similarity(gt_normalized, pred_normalized)

        return {
            'jaccard_similarity': jaccard,
            'gt_items': gt_items,
            'pred_items': pred_items,
            'gt_normalized': gt_normalized,
            'pred_normalized': pred_normalized,
            'exact_match': jaccard == 1.0
        }

    @staticmethod
    def _calculate_flexible_similarity(gt_normalized: List[str], pred_normalized: List[str]) -> float:
        """
        Calculate similarity with flexibility for aliases

        This method allows partial matches when expressions are the same but aliases differ
        """
        if len(gt_normalized) != len(pred_normalized):
            # Different number of items - use standard Jaccard
            gt_set = set(gt_normalized)
            pred_set = set(pred_normalized)
            intersection = len(gt_set.intersection(pred_set))
            union = len(gt_set.union(pred_set))
            return intersection / union if union > 0 else 0.0

        # Same number of items - try to match each item flexibly
        total_similarity = 0.0

        for gt_item in gt_normalized:
            best_match = 0.0

            for pred_item in pred_normalized:
                similarity = ImprovedColumnComparator._compare_individual_items(gt_item, pred_item)
                best_match = max(best_match, similarity)

            total_similarity += best_match

        return total_similarity / len(gt_normalized) if gt_normalized else 0.0

    @staticmethod
    def _compare_individual_items(gt_item: str, pred_item: str) -> float:
        """
        Compare two individual items with alias flexibility

        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Exact match
        if gt_item == pred_item:
            return 1.0

        # Check if both have aliases
        gt_has_alias = " AS <alias>" in gt_item
        pred_has_alias = " AS <alias>" in pred_item

        if gt_has_alias and pred_has_alias:
            # Both have aliases - compare base expressions
            gt_base = gt_item.replace(" AS <alias>", "")
            pred_base = pred_item.replace(" AS <alias>", "")
            return 1.0 if gt_base == pred_base else 0.0

        elif gt_has_alias and not pred_has_alias:
            # GT has alias, pred doesn't
            gt_base = gt_item.replace(" AS <alias>", "")
            return 0.7 if gt_base == pred_item else 0.0  # Partial match

        elif not gt_has_alias and pred_has_alias:
            # Pred has alias, GT doesn't
            pred_base = pred_item.replace(" AS <alias>", "")
            return 0.7 if gt_item == pred_base else 0.0  # Partial match

        else:
            # Neither has alias - direct comparison
            return 1.0 if gt_item == pred_item else 0.0