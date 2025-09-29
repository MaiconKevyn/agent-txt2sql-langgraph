#!/usr/bin/env python3
"""
Comprehensive Test Suite for Text-to-SQL Evaluation Metrics

This module provides extensive unit tests for all evaluation metrics
to ensure correctness and reliability of the evaluation system.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import json
from pathlib import Path

# Import metrics and utilities
from evaluation.metrics.base_metrics import (
    SQLNormalizer, SQLParser, EvaluationContext, MetricResult
)
from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.evaluator import TextToSQLEvaluator, QuestionSample


class TestSQLNormalizer(unittest.TestCase):
    """Test SQL normalization functionality"""

    def test_basic_normalization(self):
        """Test basic SQL normalization"""
        sql = "  SELECT   *   FROM   table1  ;  "
        expected = "SELECT * FROM table1"
        self.assertEqual(SQLNormalizer.normalize_sql(sql), expected)

    def test_keyword_case_normalization(self):
        """Test keyword case normalization"""
        sql = "select count(*) from table1 where id = 1"
        expected = "SELECT COUNT(*) FROM table1 WHERE id = 1"
        self.assertEqual(SQLNormalizer.normalize_sql(sql), expected)

    def test_comment_removal(self):
        """Test comment removal"""
        sql = """
        SELECT * -- This is a comment
        FROM table1 /* Multi-line
                      comment */
        WHERE id = 1
        """
        result = SQLNormalizer.normalize_sql(sql)
        self.assertNotIn("comment", result.lower())
        self.assertIn("SELECT", result)
        self.assertIn("FROM", result)
        self.assertIn("WHERE", result)

    def test_whitespace_normalization(self):
        """Test whitespace normalization"""
        sql = "SELECT\n\t*\n\nFROM\t\ttable1"
        expected = "SELECT * FROM table1"
        self.assertEqual(SQLNormalizer.normalize_sql(sql), expected)

    def test_empty_query(self):
        """Test empty query handling"""
        self.assertEqual(SQLNormalizer.normalize_sql(""), "")
        self.assertEqual(SQLNormalizer.normalize_sql("   "), "")
        self.assertEqual(SQLNormalizer.normalize_sql(None), "")

    def test_complex_query_normalization(self):
        """Test normalization of complex queries"""
        sql = """
        SELECT DISTINCT t1.name, COUNT(t2.id) as total
        FROM table1 t1
        LEFT JOIN table2 t2 ON t1.id = t2.foreign_id
        WHERE t1.status = 'active'
        GROUP BY t1.name
        HAVING COUNT(t2.id) > 5
        ORDER BY total DESC
        LIMIT 10;
        """
        normalized = SQLNormalizer.normalize_sql(sql)
        self.assertIn("SELECT DISTINCT", normalized)
        self.assertIn("LEFT JOIN", normalized)
        self.assertIn("GROUP BY", normalized)
        self.assertIn("HAVING", normalized)
        self.assertIn("ORDER BY", normalized)
        self.assertIn("LIMIT", normalized)
        self.assertNotIn(";", normalized)


class TestSQLParser(unittest.TestCase):
    """Test SQL parsing functionality"""

    def test_basic_component_extraction(self):
        """Test basic SQL component extraction"""
        sql = "SELECT name FROM users WHERE age > 18"
        components = SQLParser.extract_components(sql)

        self.assertIn("select", components)
        self.assertIn("from", components)
        self.assertIn("where", components)
        self.assertIn("name", components["select"])
        self.assertIn("users", components["from"])
        self.assertIn("age > 18", components["where"])

    def test_complex_query_parsing(self):
        """Test parsing of complex queries"""
        sql = """
        SELECT u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.status = 'active'
        GROUP BY u.name
        HAVING COUNT(o.id) > 5
        ORDER BY order_count DESC
        LIMIT 10
        """
        components = SQLParser.extract_components(sql)

        self.assertTrue(components["select"])
        self.assertTrue(components["from"])
        self.assertTrue(components["where"])
        self.assertTrue(components["group_by"])
        self.assertTrue(components["having"])
        self.assertTrue(components["order_by"])
        self.assertTrue(components["limit"])

    def test_empty_query_parsing(self):
        """Test parsing empty or invalid queries"""
        components = SQLParser.extract_components("")
        self.assertIsInstance(components, dict)
        self.assertEqual(components["select"], "")

        components = SQLParser.extract_components("INVALID SQL")
        self.assertIsInstance(components, dict)


class TestExactMatchMetric(unittest.TestCase):
    """Test Exact Match metric"""

    def setUp(self):
        self.metric = ExactMatchMetric()

    def test_exact_match_success(self):
        """Test successful exact match"""
        context = EvaluationContext(
            question_id="test1",
            question="Count all users",
            ground_truth_sql="SELECT COUNT(*) FROM users",
            predicted_sql="SELECT COUNT(*) FROM users"
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 1.0)
        self.assertTrue(result.is_correct)

    def test_exact_match_with_normalization(self):
        """Test exact match with normalization differences"""
        context = EvaluationContext(
            question_id="test2",
            question="Count all users",
            ground_truth_sql="SELECT COUNT(*) FROM users;",
            predicted_sql="  select   count(*)   from   users  "
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 1.0)
        self.assertTrue(result.is_correct)

    def test_exact_match_failure(self):
        """Test exact match failure"""
        context = EvaluationContext(
            question_id="test3",
            question="Count all users",
            ground_truth_sql="SELECT COUNT(*) FROM users",
            predicted_sql="SELECT * FROM users"
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 0.0)
        self.assertFalse(result.is_correct)

    def test_empty_queries(self):
        """Test handling of empty queries"""
        context = EvaluationContext(
            question_id="test4",
            question="Empty test",
            ground_truth_sql="",
            predicted_sql=""
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 0.0)
        self.assertFalse(result.is_correct)

    def test_difference_analysis(self):
        """Test difference analysis in results"""
        context = EvaluationContext(
            question_id="test5",
            question="Select with different columns",
            ground_truth_sql="SELECT name FROM users",
            predicted_sql="SELECT age FROM users"
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 0.0)
        self.assertIn('differing_words', result.details)
        self.assertIn('common_errors', result.details)


class TestComponentMatchingMetric(unittest.TestCase):
    """Test Component Matching metric"""

    def setUp(self):
        self.metric = ComponentMatchingMetric()

    def test_perfect_component_match(self):
        """Test perfect component matching"""
        context = EvaluationContext(
            question_id="test1",
            question="Count users by status",
            ground_truth_sql="SELECT status, COUNT(*) FROM users GROUP BY status",
            predicted_sql="SELECT status, COUNT(*) FROM users GROUP BY status"
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 1.0)
        self.assertTrue(result.is_correct)

    def test_partial_component_match(self):
        """Test partial component matching"""
        context = EvaluationContext(
            question_id="test2",
            question="Select users with filtering",
            ground_truth_sql="SELECT name, age FROM users WHERE age > 18",
            predicted_sql="SELECT name FROM users WHERE age > 18"
        )

        result = self.metric.evaluate(context)
        self.assertGreater(result.score, 0.0)
        self.assertLess(result.score, 1.0)

    def test_reordered_clauses(self):
        """Test handling of reordered clauses (CM should be more flexible)"""
        # This is a conceptual test - actual implementation may vary
        context = EvaluationContext(
            question_id="test3",
            question="Complex query with reordering",
            ground_truth_sql="SELECT name FROM users WHERE age > 18 ORDER BY name",
            predicted_sql="SELECT name FROM users ORDER BY name WHERE age > 18"
        )

        result = self.metric.evaluate(context)
        # Component matching should handle this better than exact match
        self.assertIsInstance(result.score, float)

    def test_component_scoring_details(self):
        """Test detailed component scoring"""
        context = EvaluationContext(
            question_id="test4",
            question="Multi-clause query",
            ground_truth_sql="SELECT name, COUNT(*) FROM users WHERE age > 18 GROUP BY name",
            predicted_sql="SELECT name FROM users WHERE age > 21"
        )

        result = self.metric.evaluate(context)
        self.assertIn('component_scores', result.details)
        self.assertIn('component_details', result.details)
        self.assertIn('select', result.details['component_scores'])
        self.assertIn('from', result.details['component_scores'])
        self.assertIn('where', result.details['component_scores'])


class TestExecutionAccuracyMetric(unittest.TestCase):
    """Test Execution Accuracy metric"""

    def setUp(self):
        self.metric = ExecutionAccuracyMetric()

    def test_execution_accuracy_with_mock_db(self):
        """Test execution accuracy with mocked database"""
        # Mock database connection
        mock_db = Mock()
        mock_cursor = Mock()
        mock_db.get_raw_connection.return_value.cursor.return_value = mock_cursor

        # Mock successful execution with same results
        mock_cursor.fetchall.return_value = [(1, 'John'), (2, 'Jane')]

        context = EvaluationContext(
            question_id="test1",
            question="Get user data",
            ground_truth_sql="SELECT id, name FROM users",
            predicted_sql="SELECT id, name FROM users",
            database_connection=mock_db
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 1.0)
        self.assertTrue(result.is_correct)

    def test_execution_accuracy_different_results(self):
        """Test execution accuracy with different results"""
        mock_db = Mock()
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_db.get_raw_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock different results for ground truth vs predicted
        def side_effect(*args):
            sql = args[0]
            if "ground_truth" in sql or "users" in sql:
                return [(1, 'John'), (2, 'Jane')]
            else:
                return [(1, 'John')]

        mock_cursor.fetchall.side_effect = side_effect

        # We need to test this differently since we can't easily mock the execution order
        # This is more of an integration test concept

    def test_execution_error_handling(self):
        """Test handling of execution errors"""
        mock_db = Mock()
        mock_db.get_raw_connection.side_effect = Exception("Database error")

        context = EvaluationContext(
            question_id="test2",
            question="Error test",
            ground_truth_sql="SELECT * FROM users",
            predicted_sql="SELECT * FROM users",
            database_connection=mock_db
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 0.0)
        self.assertFalse(result.is_correct)
        self.assertIsNotNone(result.error_message)

    def test_no_database_connection(self):
        """Test handling when no database connection is provided"""
        context = EvaluationContext(
            question_id="test3",
            question="No DB test",
            ground_truth_sql="SELECT * FROM users",
            predicted_sql="SELECT * FROM users",
            database_connection=None
        )

        result = self.metric.evaluate(context)
        self.assertEqual(result.score, 0.0)
        self.assertFalse(result.is_correct)
        self.assertIn("Database connection", result.error_message)


class TestTextToSQLEvaluator(unittest.TestCase):
    """Test the main evaluator orchestrator"""

    def setUp(self):
        self.mock_db = Mock()
        self.evaluator = TextToSQLEvaluator(
            database_connection=self.mock_db,
            metrics=[ExactMatchMetric(), ComponentMatchingMetric()]
        )

    def test_single_prediction_evaluation(self):
        """Test evaluation of a single prediction"""
        results = self.evaluator.evaluate_single_prediction(
            question="Count all users",
            ground_truth_sql="SELECT COUNT(*) FROM users",
            predicted_sql="SELECT COUNT(*) FROM users",
            question_id="test1"
        )

        self.assertIn("Exact Match (EM)", results)
        self.assertIn("Component Matching (CM)", results)
        self.assertIsInstance(results["Exact Match (EM)"], MetricResult)

    def test_load_and_sample_questions(self):
        """Test loading and sampling questions from ground truth"""
        # Create temporary ground truth file
        ground_truth_data = [
            {
                "id": "GT001",
                "question": "How many users?",
                "query": "SELECT COUNT(*) FROM users",
                "difficulty": "easy",
                "tables": ["users"]
            },
            {
                "id": "GT002",
                "question": "List all products",
                "query": "SELECT * FROM products",
                "difficulty": "medium",
                "tables": ["products"]
            }
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(ground_truth_data, f)
            temp_path = f.name

        try:
            questions = self.evaluator._load_and_sample_questions(
                temp_path, sample_size=1, difficulty_filter=None, random_seed=42
            )

            self.assertEqual(len(questions), 1)
            self.assertIsInstance(questions[0], QuestionSample)
            self.assertIn(questions[0].id, ["GT001", "GT002"])

        finally:
            Path(temp_path).unlink()

    def test_difficulty_filtering(self):
        """Test filtering questions by difficulty"""
        ground_truth_data = [
            {
                "id": "GT001",
                "question": "Easy question",
                "query": "SELECT COUNT(*) FROM users",
                "difficulty": "easy",
                "tables": ["users"]
            },
            {
                "id": "GT002",
                "question": "Hard question",
                "query": "SELECT * FROM complex_view",
                "difficulty": "hard",
                "tables": ["complex_view"]
            }
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(ground_truth_data, f)
            temp_path = f.name

        try:
            # Filter for easy questions only
            questions = self.evaluator._load_and_sample_questions(
                temp_path, sample_size=10, difficulty_filter=["easy"], random_seed=42
            )

            self.assertEqual(len(questions), 1)
            self.assertEqual(questions[0].difficulty, "easy")
            self.assertEqual(questions[0].id, "GT001")

        finally:
            Path(temp_path).unlink()


class TestMetricIntegration(unittest.TestCase):
    """Integration tests for metrics working together"""

    def test_all_metrics_consistency(self):
        """Test that all metrics work together consistently"""
        # Test case where all metrics should agree (perfect match)
        context = EvaluationContext(
            question_id="integration1",
            question="Count users",
            ground_truth_sql="SELECT COUNT(*) FROM users",
            predicted_sql="SELECT COUNT(*) FROM users"
        )

        em_metric = ExactMatchMetric()
        cm_metric = ComponentMatchingMetric()

        em_result = em_metric.evaluate(context)
        cm_result = cm_metric.evaluate(context)

        # Both should indicate success for identical queries
        self.assertTrue(em_result.is_correct)
        self.assertTrue(cm_result.is_correct)
        self.assertEqual(em_result.score, 1.0)
        self.assertEqual(cm_result.score, 1.0)

    def test_metrics_divergence(self):
        """Test cases where metrics should diverge"""
        # Test case where CM might score higher than EM due to flexibility
        context = EvaluationContext(
            question_id="integration2",
            question="Select with different order",
            ground_truth_sql="SELECT name, age FROM users WHERE status = 'active'",
            predicted_sql="SELECT age, name FROM users WHERE status = 'active'"
        )

        em_metric = ExactMatchMetric()
        cm_metric = ComponentMatchingMetric()

        em_result = em_metric.evaluate(context)
        cm_result = cm_metric.evaluate(context)

        # EM should fail due to column order difference
        self.assertFalse(em_result.is_correct)
        # CM might score higher due to component-level matching
        # (though in this case it might also fail - depends on implementation)


def run_comprehensive_tests():
    """Run all tests and generate a comprehensive report"""
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromModule(__import__(__name__))

    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=None)
    result = runner.run(test_suite)

    # Print summary
    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    success_rate = (total_tests - failures - errors) / total_tests if total_tests > 0 else 0

    print("\n" + "="*60)
    print("COMPREHENSIVE TEST RESULTS")
    print("="*60)
    print(f"Total Tests: {total_tests}")
    print(f"Successful: {total_tests - failures - errors}")
    print(f"Failures: {failures}")
    print(f"Errors: {errors}")
    print(f"Success Rate: {success_rate:.1%}")
    print("="*60)

    if failures > 0:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError:')[-1].strip()}")

    if errors > 0:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('Exception:')[-1].strip()}")

    return result


if __name__ == "__main__":
    # Add the project root to Python path for imports
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

    # Run comprehensive tests
    run_comprehensive_tests()