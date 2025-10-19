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
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from evaluation.dag import create_evaluation_pipeline


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
