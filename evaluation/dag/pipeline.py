"""
Evaluation Pipeline DAG Definition

This module defines the complete evaluation pipeline as a DAG using NetworkX.
The pipeline includes all steps from configuration loading to results saving.
"""

from .base import EvaluationDAG
from . import tasks


def create_evaluation_pipeline() -> EvaluationDAG:
    """
    Create the complete evaluation pipeline DAG

    Pipeline structure:

    1. load_configuration (entry point)
    2. load_ground_truth (parallel with config)
    3. initialize_database (depends on config)
    4. initialize_metrics (depends on config)
    5. initialize_agent (depends on config)
    6. evaluate_questions (depends on ground_truth, metrics, agent, database)
    7. aggregate_results (depends on evaluation)
    8. generate_report (depends on aggregation)
    9. save_results (depends on evaluation, aggregation, report)
    10. cleanup_resources (depends on database, runs at end)

    Returns:
        Configured EvaluationDAG instance
    """
    dag = EvaluationDAG(name="Text-to-SQL Evaluation Pipeline")

    # ========================================================================
    # Stage 1: Initialization (parallel tasks)
    # ========================================================================

    dag.add_task(
        name="load_configuration",
        func=tasks.load_configuration,
        depends_on=[],
        description="Load application and LLM configuration"
    )

    dag.add_task(
        name="load_ground_truth",
        func=tasks.load_ground_truth,
        depends_on=[],
        description="Load ground truth questions from JSON"
    )

    # ========================================================================
    # Stage 2: Setup (depends on configuration)
    # ========================================================================

    dag.add_task(
        name="initialize_database",
        func=tasks.initialize_database,
        depends_on=["load_configuration"],
        description="Initialize PostgreSQL database connection"
    )

    dag.add_task(
        name="initialize_metrics",
        func=tasks.initialize_metrics,
        depends_on=["load_configuration"],
        description="Initialize EM, CM, and EX metrics"
    )

    dag.add_task(
        name="initialize_agent",
        func=tasks.initialize_agent,
        depends_on=["load_configuration"],
        description="Initialize LangGraph agent orchestrator"
    )

    # ========================================================================
    # Stage 3: Evaluation (depends on all setup tasks)
    # ========================================================================

    dag.add_task(
        name="evaluate_questions",
        func=tasks.evaluate_questions,
        depends_on=[
            "load_ground_truth",
            "initialize_metrics",
            "initialize_agent",
            "initialize_database"
        ],
        description="Evaluate all questions with agent and metrics"
    )

    # ========================================================================
    # Stage 4: Analysis (depends on evaluation)
    # ========================================================================

    dag.add_task(
        name="aggregate_results",
        func=tasks.aggregate_results,
        depends_on=["evaluate_questions"],
        description="Aggregate results and calculate statistics"
    )

    dag.add_task(
        name="generate_report",
        func=tasks.generate_report,
        depends_on=["aggregate_results", "evaluate_questions"],
        description="Generate human-readable evaluation report"
    )

    # ========================================================================
    # Stage 5: Output (depends on all analysis)
    # ========================================================================

    dag.add_task(
        name="save_results",
        func=tasks.save_results,
        depends_on=[
            "evaluate_questions",
            "aggregate_results",
            "generate_report",
            "load_configuration",
            "initialize_agent",
            "initialize_database"
        ],
        description="Save results to JSON and print report"
    )

    # ========================================================================
    # Stage 6: Cleanup (final task)
    # ========================================================================

    dag.add_task(
        name="cleanup_resources",
        func=tasks.cleanup_resources,
        depends_on=["initialize_database", "save_results"],
        description="Close database connections and cleanup"
    )

    return dag


def create_sample_evaluation_pipeline(sample_size: int = 10) -> EvaluationDAG:
    """
    Create a smaller evaluation pipeline for testing/sampling

    Args:
        sample_size: Number of questions to evaluate

    Returns:
        Configured EvaluationDAG instance for sampling
    """
    # For now, use the same pipeline
    # In the future, could add sampling logic
    dag = create_evaluation_pipeline()
    dag.name = f"Text-to-SQL Sample Evaluation (n={sample_size})"

    return dag


def visualize_pipeline(output_path: str = "evaluation/workflow.png") -> None:
    """
    Generate and save visualization of the evaluation pipeline

    Args:
        output_path: Path to save the visualization
    """
    dag = create_evaluation_pipeline()

    print(f"\n{'='*80}")
    print("GENERATING PIPELINE VISUALIZATION")
    print(f"{'='*80}\n")

    dag.print_summary()
    dag.visualize(output_path=output_path, show_descriptions=True)

    print(f"\n✅ Pipeline visualization saved to: {output_path}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    # Generate visualization when run directly
    visualize_pipeline()
