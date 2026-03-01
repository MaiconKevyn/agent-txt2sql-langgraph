#!/usr/bin/env python3
"""
DAG-Based Evaluation Runner

This script runs the complete evaluation pipeline using a DAG (Directed Acyclic Graph)
for better organization, visualization, and maintainability.

Usage:
    # Run full evaluation
    python evaluation/run_dag_evaluation.py

    # Generate visualization only
    python evaluation/run_dag_evaluation.py --visualize-only

    # Run with custom output path
    python evaluation/run_dag_evaluation.py --output results/custom_eval.json
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from evaluation.dag import create_evaluation_pipeline


def export_ex_zero_failures(
    dag_results: Dict[str, Any],
    output_dir: Path
) -> Optional[Path]:
    """
    Generate a text file listing only the ground truths with EX score == 0.

    Args:
        dag_results: Results returned by dag.execute()
        output_dir: Directory where the file should be written

    Returns:
        Path to the generated file, or None if no failures were found/created
    """

    eval_task = dag_results.get("evaluate_questions")

    if not eval_task or not getattr(eval_task, "success", False):
        print("⚠️  EX=0 export skipped: evaluate_questions task not available or failed")
        return None

    detailed_results = eval_task.data.get("detailed_results", []) if eval_task.data else []

    ex_zero_entries: List[Tuple[str, str]] = []

    for item in detailed_results:
        metrics = item.get("metrics", {})
        ex_metric = metrics.get("Execution Accuracy (EX)")

        # Skip if metric is missing
        if ex_metric is None:
            continue

        score = ex_metric.get("score", 0)

        try:
            score_float = float(score)
        except (TypeError, ValueError):
            score_float = 0.0

        if score_float == 0.0:
            question_id = item.get("question_id") or item.get("id") or "UNKNOWN_ID"
            question_text = item.get("question", "").strip()
            ex_zero_entries.append((str(question_id), question_text))

    if not ex_zero_entries:
        print("✅ Nenhum ground truth com EX = 0; arquivo não gerado")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ex_zero_ground_truth_{timestamp}.txt"

    lines = ["Ground truths com EX = 0", "----------------------------------------"]
    for qid, question in ex_zero_entries:
        if question:
            lines.append(f"{qid} | {question} | EX = 0")
        else:
            lines.append(f"{qid} | EX = 0")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"⚠️  {len(ex_zero_entries)} ground truths com EX = 0")
    print(f"    Lista salva em: {output_path}")

    return output_path


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run Text-to-SQL evaluation using DAG-based pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full evaluation
  python evaluation/run_dag_evaluation.py

  # Generate visualization only (no execution)
  python evaluation/run_dag_evaluation.py --visualize-only

  # Run and save visualization
  python evaluation/run_dag_evaluation.py --save-dag-visualization
        """
    )

    parser.add_argument(
        "--visualize-only",
        action="store_true",
        help="Only generate DAG visualization without running evaluation"
    )

    parser.add_argument(
        "--save-dag-visualization",
        action="store_true",
        help="Save DAG visualization after execution"
    )

    parser.add_argument(
        "--dag-output",
        type=str,
        default="docs/evaluation_pipeline_dag.png",
        help="Path to save DAG visualization (default: docs/evaluation_pipeline_dag.png)"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()

    print("\n" + "="*80)
    print("TEXT-TO-SQL EVALUATION - DAG-BASED PIPELINE")
    print("="*80 + "\n")

    try:
        # Create pipeline DAG
        print("Creating evaluation pipeline DAG...")
        dag = create_evaluation_pipeline()

        # Validate DAG structure
        if not dag.validate():
            print("❌ DAG validation failed - cannot proceed")
            sys.exit(1)

        print("✅ DAG created and validated successfully\n")

        # Print pipeline summary
        if args.verbose:
            dag.print_summary()

        # Visualize only mode
        if args.visualize_only:
            print(f"Generating visualization: {args.dag_output}")
            dag.visualize(output_path=args.dag_output, show_descriptions=True)
            print(f"✅ Visualization saved to: {args.dag_output}")
            print("\nNote: Use without --visualize-only to run evaluation")
            return

        # Execute pipeline
        print("Starting pipeline execution...\n")
        start_time = datetime.now()

        results = dag.execute()

        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()

        # Check execution results
        successful_tasks = sum(1 for r in results.values() if r.success)
        failed_tasks = len(results) - successful_tasks

        print(f"\n{'='*80}")
        print("PIPELINE EXECUTION SUMMARY")
        print(f"{'='*80}")
        print(f"Total time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
        print(f"Successful tasks: {successful_tasks}/{len(results)}")
        print(f"Failed tasks: {failed_tasks}/{len(results)}")

        if failed_tasks > 0:
            print("\n⚠️  Some tasks failed:")
            for task_name, result in results.items():
                if not result.success:
                    print(f"  - {task_name}: {result.error}")

        # Save DAG visualization if requested
        if args.save_dag_visualization:
            print(f"\nSaving DAG visualization: {args.dag_output}")
            dag.visualize(output_path=args.dag_output, show_descriptions=True)
            print(f"✅ DAG visualization saved")

        # Export ground truths com EX = 0
        export_ex_zero_failures(
            dag_results=results,
            output_dir=project_root / "evaluation" / "results"
        )

        # Final status
        print(f"\n{'='*80}")
        if failed_tasks == 0:
            print("✅ EVALUATION COMPLETED SUCCESSFULLY")
            print(f"{'='*80}\n")

            # Print summary from save_results task
            if 'save_results' in results and results['save_results'].success:
                save_data = results['save_results'].data
                print(f"📊 Results saved to:")
                print(f"   - JSON: {save_data['json_path']}")
                print(f"   - Report: {save_data['report_path']}")

        else:
            print("⚠️  EVALUATION COMPLETED WITH ERRORS")
            print(f"{'='*80}\n")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Evaluation interrupted by user")
        sys.exit(130)

    except Exception as e:
        print(f"\n❌ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
