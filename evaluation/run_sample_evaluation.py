#!/usr/bin/env python3
"""
Sample Evaluation Script for Text-to-SQL Metrics

This script runs evaluation on a 10-question sample from ground truth
to validate the metrics implementation.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from evaluation.metrics.exact_match import ExactMatchMetric
from evaluation.metrics.component_matching import ComponentMatchingMetric
from evaluation.metrics.execution_accuracy import ExecutionAccuracyMetric
from evaluation.metrics.base_metrics import EvaluationContext


class MockAgentOrchestrator:
    """Mock agent for testing that generates realistic SQL variations"""

    def __init__(self):
        self.question_count = 0

    def process_request(self, question: str) -> str:
        """Generate mock SQL responses with realistic variations"""
        self.question_count += 1

        # Mock some realistic agent behaviors
        if "quantas internações" in question.lower():
            return "SELECT COUNT(*) FROM internacoes;"
        elif "quantas mortes" in question.lower():
            return "SELECT COUNT(*) FROM mortes;"
        elif "procedimentos" in question.lower():
            return "SELECT COUNT(*) FROM procedimentos;"
        elif "cid" in question.lower():
            return "SELECT COUNT(*) FROM cid10;"
        elif "hospitais" in question.lower():
            return "SELECT COUNT(*) FROM hospital;"
        elif "masculino" in question.lower():
            return "SELECT COUNT(*) FROM internacoes WHERE SEXO = 1;"
        elif "feminino" in question.lower() or "mulheres" in question.lower():
            return "SELECT COUNT(*) FROM internacoes WHERE SEXO = 3;"
        elif "cardiovasculares" in question.lower():
            return "SELECT COUNT(*) FROM mortes WHERE CID_MORTE LIKE 'I%';"
        elif "descrição" in question.lower() and "a15" in question.lower():
            return "SELECT CD_DESCRICAO FROM cid10 WHERE CID = 'A15';"
        else:
            # For more complex queries, simulate some variations
            if self.question_count % 3 == 0:
                return "SELECT COUNT(*) FROM internacoes;"  # Wrong table sometimes
            else:
                return "SELECT COUNT(*) FROM internacoes;"  # Default response


def load_ground_truth_sample(file_path: Path, sample_size: int = 10) -> list:
    """Load a sample from ground truth data"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Take first 10 questions for reproducible testing
    return data[:sample_size]


def run_sample_evaluation():
    """Run evaluation on 10-question sample"""
    print("="*60)
    print("TEXT-TO-SQL METRICS - SAMPLE EVALUATION")
    print("="*60)

    # Load ground truth sample
    gt_path = project_root / "evaluation" / "ground_truth.json"
    if not gt_path.exists():
        print(f"❌ Ground truth file not found: {gt_path}")
        return

    sample_questions = load_ground_truth_sample(gt_path, 10)
    print(f"✅ Loaded {len(sample_questions)} questions from ground truth")

    # Initialize metrics
    metrics = [
        ExactMatchMetric(),
        ComponentMatchingMetric(),
        ExecutionAccuracyMetric()  # Will skip execution due to no DB
    ]
    print(f"✅ Initialized {len(metrics)} metrics")

    # Mock agent
    agent = MockAgentOrchestrator()

    # Results storage
    results = []
    metric_scores = {metric.name: [] for metric in metrics}

    print("\n" + "="*60)
    print("EVALUATING QUESTIONS")
    print("="*60)

    # Evaluate each question
    for i, question_data in enumerate(sample_questions, 1):
        print(f"\n[{i:2d}/10] Question: {question_data['id']}")
        print(f"        Text: {question_data['question'][:60]}...")

        # Get agent prediction
        predicted_sql = agent.process_request(question_data['question'])

        # Create evaluation context
        context = EvaluationContext(
            question_id=question_data['id'],
            question=question_data['question'],
            ground_truth_sql=question_data['query'],
            predicted_sql=predicted_sql,
            database_connection=None  # Skip execution for now
        )

        # Evaluate with each metric
        question_results = {
            'question_id': question_data['id'],
            'question': question_data['question'],
            'ground_truth_sql': question_data['query'],
            'predicted_sql': predicted_sql,
            'metrics': {}
        }

        for metric in metrics:
            if metric.name == "Execution Accuracy (EX)":
                # Skip execution accuracy without DB
                print(f"        {metric.name}: SKIPPED (no database)")
                continue

            result = metric.evaluate(context)
            question_results['metrics'][metric.name] = {
                'score': result.score,
                'is_correct': result.is_correct,
                'error': result.error_message
            }
            metric_scores[metric.name].append(result.score)

            status = "✅" if result.is_correct else "❌"
            print(f"        {metric.name}: {result.score:.3f} {status}")

        results.append(question_results)

    # Calculate summary statistics
    print("\n" + "="*60)
    print("EVALUATION SUMMARY")
    print("="*60)

    total_questions = len(sample_questions)

    for metric_name, scores in metric_scores.items():
        if scores:  # Only if we have scores
            avg_score = sum(scores) / len(scores)
            accuracy = sum(1 for s in scores if s >= 0.8) / len(scores)
            perfect_matches = sum(1 for s in scores if s == 1.0)

            print(f"\n{metric_name}:")
            print(f"  Average Score: {avg_score:.3f}")
            print(f"  Accuracy (≥0.8): {accuracy:.1%} ({int(accuracy * len(scores))}/{len(scores)})")
            print(f"  Perfect Matches: {perfect_matches}/{len(scores)} ({perfect_matches/len(scores):.1%})")

    # Detailed results
    print(f"\nDETAILED RESULTS:")
    print("-" * 60)

    for result in results:
        qid = result['question_id']
        print(f"\n{qid}: {result['question'][:50]}...")
        print(f"  GT:   {result['ground_truth_sql'][:60]}...")
        print(f"  Pred: {result['predicted_sql'][:60]}...")

        for metric_name, metric_result in result['metrics'].items():
            score = metric_result['score']
            correct = metric_result['is_correct']
            status = "✅" if correct else "❌"
            print(f"  {metric_name}: {score:.3f} {status}")

    # Save results
    results_data = {
        'evaluation_timestamp': datetime.now().isoformat(),
        'total_questions': total_questions,
        'summary': {
            'metric_scores': {name: sum(scores)/len(scores) if scores else 0
                            for name, scores in metric_scores.items()},
            'metric_accuracy': {name: sum(1 for s in scores if s >= 0.8)/len(scores) if scores else 0
                              for name, scores in metric_scores.items()}
        },
        'detailed_results': results
    }

    output_path = project_root / "evaluation" / "results" / "sample_evaluation_results.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Results saved to: {output_path}")
    print("="*60)

    return results_data


if __name__ == "__main__":
    try:
        results = run_sample_evaluation()
        print("\n🎉 Sample evaluation completed successfully!")
    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)