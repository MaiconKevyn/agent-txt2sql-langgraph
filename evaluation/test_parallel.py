#!/usr/bin/env python3
"""
Test Parallel Evaluation

Quick test script to compare sequential vs parallel evaluation performance.
Tests with a small subset of questions to avoid long runtime.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from evaluation.dag import create_evaluation_pipeline


def test_parallel_evaluation(max_workers: int = 2, num_questions: int = 5):
    """
    Test evaluation with specified number of workers

    Args:
        max_workers: Number of parallel workers (1=sequential)
        num_questions: Number of questions to evaluate (for quick test)
    """
    print(f"\n{'='*80}")
    print(f"TESTING EVALUATION: max_workers={max_workers}, questions={num_questions}")
    print(f"{'='*80}\n")

    # Temporarily modify ground truth to only have first N questions
    import json
    from pathlib import Path

    project_root = Path(__file__).parent.parent
    gt_path = project_root / "evaluation" / "ground_truth.json"
    gt_backup_path = project_root / "evaluation" / "ground_truth_backup.json"

    # Backup original
    with open(gt_path, 'r') as f:
        original_data = json.load(f)

    # Save backup
    with open(gt_backup_path, 'w') as f:
        json.dump(original_data, f)

    # Write limited version
    limited_data = original_data[:num_questions]
    with open(gt_path, 'w') as f:
        json.dump(limited_data, f)

    try:
        # Create pipeline
        dag = create_evaluation_pipeline()

        # Modify evaluate_questions task to use max_workers
        # We'll pass it through initial_data
        initial_data = {
            'max_workers': max_workers
        }

        # Execute DAG
        results = dag.execute(initial_data=initial_data)

        # Check if evaluation succeeded
        if 'evaluate_questions' in results and results['evaluate_questions'].success:
            eval_data = results['evaluate_questions'].data
            agent_stats = eval_data['agent_stats']

            print(f"\n{'='*80}")
            print(f"RESULTS (max_workers={max_workers})")
            print(f"{'='*80}")
            print(f"Total time: {agent_stats['total_time']:.2f}s")
            print(f"Avg per query: {agent_stats['total_time']/eval_data['total_questions']:.2f}s")
            print(f"Success rate: {agent_stats['success_count']}/{eval_data['total_questions']}")
            print(f"{'='*80}\n")

            return agent_stats['total_time']
        else:
            print(f"❌ Evaluation task failed")
            return None

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        # Restore original ground truth
        import shutil
        shutil.move(str(gt_backup_path), str(gt_path))
        print(f"\n✅ Ground truth restored")


def main():
    """Run comparison tests"""
    print("\n" + "="*80)
    print("PARALLEL EVALUATION PERFORMANCE TEST")
    print("="*80)
    print("\nTesting with 5 questions to compare performance...")
    print("⚠️  Monitor GPU usage during parallel execution!")
    print("")

    # Test configurations
    tests = [
        (1, "Sequential (baseline)"),
        (2, "Parallel 2 workers (conservative)")
    ]

    times = {}

    for max_workers, description in tests:
        print(f"\n{'─'*80}")
        print(f"Test: {description}")
        print(f"{'─'*80}")

        time_taken = test_parallel_evaluation(max_workers=max_workers, num_questions=5)

        if time_taken:
            times[max_workers] = time_taken

        # Wait a bit between tests
        import time
        time.sleep(2)

    # Summary
    if len(times) >= 2:
        print(f"\n{'='*80}")
        print("PERFORMANCE COMPARISON")
        print(f"{'='*80}")

        sequential_time = times.get(1)
        parallel_time = times.get(2)

        if sequential_time and parallel_time:
            speedup = sequential_time / parallel_time
            time_saved = sequential_time - parallel_time

            print(f"\nSequential (1 worker):  {sequential_time:.2f}s")
            print(f"Parallel (2 workers):   {parallel_time:.2f}s")
            print(f"\nSpeedup: {speedup:.2f}x")
            print(f"Time saved: {time_saved:.2f}s ({time_saved/sequential_time*100:.1f}%)")

            if speedup > 1:
                print(f"\n✅ Parallel execution is FASTER!")
                print(f"   Estimated time for 52 queries:")
                print(f"   - Sequential: {sequential_time/5*52:.1f}s ({sequential_time/5*52/60:.1f} min)")
                print(f"   - Parallel:   {parallel_time/5*52:.1f}s ({parallel_time/5*52/60:.1f} min)")
            else:
                print(f"\n⚠️  Parallel execution is SLOWER or similar")
                print(f"   GPU may be limiting parallel performance")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
