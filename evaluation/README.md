# Text-to-SQL Evaluation System

This evaluation system implements standard Text-to-SQL metrics: Exact Match (EM), Component Matching (CM), and Execution Accuracy (EX).

## Architecture

The evaluation is organized as a DAG (Directed Acyclic Graph) pipeline for better maintainability and visualization:

```
evaluation/
├── dag/                       # DAG-based pipeline
│   ├── base.py               # DAG implementation
│   ├── tasks.py              # Pipeline tasks
│   └── pipeline.py           # Pipeline definition
├── metrics/                  # Metric implementations
│   ├── exact_match.py        # EM metric
│   ├── component_matching.py # CM metric
│   └── execution_accuracy.py # EX metric
├── ground_truth.json         # 55 evaluation queries
└── run_dag_evaluation.py     # Main runner
```

## DAG Pipeline

The evaluation runs through these stages:

1. Load configuration and ground truth (55 queries)
2. Initialize database, metrics (EM, CM, EX), and agent
3. Evaluate all questions using real LangGraph agent
4. Aggregate results and generate metrics
5. Save results and reports

## Running Evaluation

Execute complete evaluation on all 55 queries:

```bash
python evaluation/run_dag_evaluation.py
```

Optional flags:
- `--save-dag-visualization` - Save DAG diagram to docs/evaluation_pipeline_dag.png
- `--verbose` - Show detailed execution logs
- `--visualize-only` - Generate DAG visualization without running evaluation

## Results

Results are automatically saved to `evaluation/results/`:

- `dag_evaluation_YYYYMMDD_HHMMSS.json` - Complete results with all metrics
- `dag_evaluation_report_YYYYMMDD_HHMMSS.txt` - Summary report

## Metrics

### Exact Match (EM)
Binary score (0.0 or 1.0) requiring perfect syntactic match.

### Component Matching (CM)
Weighted score (0.0 to 1.0) evaluating individual SQL clauses. Query considered correct if CM >= 0.8.

### Execution Accuracy (EX)
Binary score (0.0 or 1.0) comparing query results on real database. Most reliable indicator of correctness.

## Database Requirements

Requires PostgreSQL connection configured in `.env`:

```
DATABASE_PATH=postgresql+psycopg2://user:password@host:port/database
```

## Ground Truth

The `ground_truth.json` contains 55 queries covering:
- Easy (19): Single table, basic operations
- Medium (18): Filtering, aggregation, temporal analysis
- Hard (18): Multi-table JOINs, complex calculations

Queries focus on real healthcare management scenarios for DATASUS/SIH-RS data.
