#!/usr/bin/env python3
"""
Test script for improved SQL parser

This script tests the corrections for:
1. WHERE clause parsing (not mixing with FROM)
2. Alias handling in column comparison
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from evaluation.metrics.improved_sql_parser import ImprovedSQLParser, ImprovedColumnComparator
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.base_metrics import EvaluationContext


def test_where_clause_parsing():
    """Test that WHERE clauses are correctly extracted"""
    print("🔍 TESTING WHERE CLAUSE PARSING")
    print("-" * 50)

    test_cases = [
        {
            "name": "Basic WHERE",
            "sql": "SELECT COUNT(*) FROM users WHERE age > 18",
            "expected_from": "users",
            "expected_where": "age > 18"
        },
        {
            "name": "Complex WHERE (GT042)",
            "sql": "SELECT COUNT(*) AS internacoes_2015 FROM internacoes WHERE EXTRACT(YEAR FROM \"DT_INTER\") = 2015",
            "expected_from": "internacoes",
            "expected_where": "EXTRACT(YEAR FROM \"DT_INTER\") = 2015"
        },
        {
            "name": "WHERE with multiple conditions",
            "sql": "SELECT name FROM users WHERE age > 18 AND status = 'active'",
            "expected_from": "users",
            "expected_where": "age > 18 AND status = 'active'"
        },
        {
            "name": "No WHERE clause",
            "sql": "SELECT COUNT(*) FROM users",
            "expected_from": "users",
            "expected_where": ""
        }
    ]

    for case in test_cases:
        print(f"\n📝 {case['name']}")
        print(f"   SQL: {case['sql']}")

        components = ImprovedSQLParser.extract_components(case['sql'])

        print(f"   FROM: '{components['from']}'")
        print(f"   WHERE: '{components['where']}'")

        # Check results
        from_ok = components['from'].strip() == case['expected_from']
        where_ok = components['where'].strip() == case['expected_where']

        status = "✅" if (from_ok and where_ok) else "❌"
        print(f"   Result: {status}")

        if not from_ok:
            print(f"   ❌ FROM mismatch: expected '{case['expected_from']}', got '{components['from']}'")
        if not where_ok:
            print(f"   ❌ WHERE mismatch: expected '{case['expected_where']}', got '{components['where']}'")


def test_alias_handling():
    """Test that different aliases are handled correctly"""
    print("\n\n🔍 TESTING ALIAS HANDLING")
    print("-" * 50)

    test_cases = [
        {
            "name": "Same expression, different aliases",
            "gt": "COUNT(*) AS total_internacoes",
            "pred": "COUNT(*) AS internacoes_2015",
            "expected_similarity": 1.0  # Should be considered equivalent
        },
        {
            "name": "Same expression, one with alias",
            "gt": "COUNT(*) AS total",
            "pred": "COUNT(*)",
            "expected_similarity": 0.5  # Partial match
        },
        {
            "name": "Different expressions",
            "gt": "COUNT(*)",
            "pred": "SUM(value)",
            "expected_similarity": 0.0  # No match
        },
        {
            "name": "Multiple columns with aliases",
            "gt": "name, COUNT(*) AS total",
            "pred": "name, COUNT(*) AS count_result",
            "expected_similarity": 1.0  # Should match (same expressions)
        }
    ]

    for case in test_cases:
        print(f"\n📝 {case['name']}")
        print(f"   GT:   {case['gt']}")
        print(f"   Pred: {case['pred']}")

        gt_items = ImprovedColumnComparator.extract_select_items(case['gt'])
        pred_items = ImprovedColumnComparator.extract_select_items(case['pred'])

        comparison = ImprovedColumnComparator.compare_select_items(gt_items, pred_items)

        print(f"   GT normalized:   {comparison['gt_normalized']}")
        print(f"   Pred normalized: {comparison['pred_normalized']}")
        print(f"   Similarity: {comparison['jaccard_similarity']:.3f}")

        # Check if similarity is reasonable
        similarity_ok = abs(comparison['jaccard_similarity'] - case['expected_similarity']) < 0.3
        status = "✅" if similarity_ok else "❌"
        print(f"   Result: {status}")


def test_component_matching_integration():
    """Test the complete Component Matching metric with improvements"""
    print("\n\n🔍 TESTING COMPONENT MATCHING INTEGRATION")
    print("-" * 50)

    # Test the specific GT042 case that was problematic
    gt_sql = "SELECT COUNT(*) AS internacoes_2015 FROM internacoes WHERE EXTRACT(YEAR FROM \"DT_INTER\") = 2015"
    pred_sql = "SELECT COUNT(*) AS total_internacoes FROM internacoes"

    print(f"Ground Truth: {gt_sql}")
    print(f"Predicted:    {pred_sql}")

    context = EvaluationContext(
        question_id="GT042",
        question="Quantas internações ocorreram em 2015?",
        ground_truth_sql=gt_sql,
        predicted_sql=pred_sql
    )

    metric = ComponentMatchingMetric()
    result = metric.evaluate(context)

    print(f"\nOverall Score: {result.score:.3f}")
    print(f"Is Correct: {result.is_correct}")

    # Show component breakdown
    if 'component_scores' in result.details:
        print("\nComponent Scores:")
        for component, score in result.details['component_scores'].items():
            print(f"  {component:12s}: {score:.3f}")

    # Show component details for key components
    if 'component_details' in result.details:
        details = result.details['component_details']

        print("\nComponent Analysis:")

        # FROM clause
        if 'from' in details:
            from_detail = details['from']
            print(f"  FROM clause:")
            print(f"    GT: '{result.details['ground_truth_components']['from']}'")
            print(f"    Pred: '{result.details['predicted_components']['from']}'")
            print(f"    Match: {from_detail.get('match', 'unknown')}")

        # WHERE clause
        if 'where' in details:
            where_detail = details['where']
            print(f"  WHERE clause:")
            print(f"    GT: '{result.details['ground_truth_components']['where']}'")
            print(f"    Pred: '{result.details['predicted_components']['where']}'")
            print(f"    Match: {where_detail.get('match', 'unknown')}")

        # SELECT clause
        if 'select' in details:
            select_detail = details['select']
            print(f"  SELECT clause:")
            print(f"    GT: '{result.details['ground_truth_components']['select']}'")
            print(f"    Pred: '{result.details['predicted_components']['select']}'")
            if 'jaccard_similarity' in select_detail:
                print(f"    Similarity: {select_detail['jaccard_similarity']:.3f}")


def main():
    """Run all tests"""
    print("🚀 TESTING IMPROVED SQL PARSER AND COMPONENT MATCHING")
    print("=" * 60)

    test_where_clause_parsing()
    test_alias_handling()
    test_component_matching_integration()

    print("\n" + "=" * 60)
    print("🎉 TESTING COMPLETED")
    print("Check the results above to verify improvements!")


if __name__ == "__main__":
    main()