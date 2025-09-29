# Text-to-SQL Evaluation Metrics System

This comprehensive evaluation system implements the three standard metrics for Text-to-SQL evaluation following the Spider benchmark methodology: **Exact Match (EM)**, **Component Matching (CM)**, and **Execution Accuracy (EX)**.

## 📊 Metrics Overview

### 1. Exact Match (EM)
- **Purpose**: Syntactic evaluation requiring perfect query structure match
- **Score Range**: 0.0 or 1.0 (binary)
- **Features**:
  - SQL normalization (whitespace, case, comments)
  - Strict structural comparison
  - Detailed difference analysis

### 2. Component Matching (CM)
- **Purpose**: Clause-level evaluation allowing structural flexibility
- **Score Range**: 0.0 to 1.0 (weighted average)
- **Features**:
  - Independent clause evaluation (SELECT, FROM, WHERE, etc.)
  - Partial credit for correct components
  - Clause reordering tolerance

### 3. Execution Accuracy (EX)
- **Purpose**: Semantic evaluation through result set comparison
- **Score Range**: 0.0 or 1.0 (binary)
- **Features**:
  - Database execution with timeout protection
  - Result set normalization and comparison
  - Most reliable indicator of user-facing accuracy

## 🏗️ System Architecture

```
evaluation/
├── metrics/                    # Core metric implementations
│   ├── __init__.py
│   ├── base_metrics.py        # Abstract classes and utilities
│   ├── exact_match.py         # EM metric implementation
│   ├── component_matching.py  # CM metric implementation
│   └── execution_accuracy.py  # EX metric implementation
├── runners/                   # CLI interfaces
│   ├── __init__.py
│   └── evaluation_runner.py   # Main CLI runner
├── evaluator.py              # Main orchestrator
├── test_metrics.py           # Comprehensive test suite
├── run_sample_evaluation.py  # Sample evaluation script
└── README.md                 # This documentation
```

## 🚀 Quick Start

### Basic Usage

```python
from evaluation.metrics import ExactMatchMetric, ComponentMatchingMetric
from evaluation.metrics.base_metrics import EvaluationContext

# Create evaluation context
context = EvaluationContext(
    question_id="Q001",
    question="How many users are there?",
    ground_truth_sql="SELECT COUNT(*) FROM users",
    predicted_sql="SELECT COUNT(*) FROM users"
)

# Evaluate with metrics
em_metric = ExactMatchMetric()
cm_metric = ComponentMatchingMetric()

em_result = em_metric.evaluate(context)
cm_result = cm_metric.evaluate(context)

print(f"EM Score: {em_result.score}")
print(f"CM Score: {cm_result.score}")
```

### Sample Evaluation (10 Questions)

```bash
# Run sample evaluation on first 10 ground truth questions
source .venv/bin/activate
python evaluation/run_sample_evaluation.py
```

### CLI Runner (Full Evaluation)

```bash
# Full evaluation with database connection
source .venv/bin/activate
python -m evaluation.runners.evaluation_runner \
    --ground-truth evaluation/ground_truth.json \
    --output results/evaluation_results.json \
    --sample-size 20 \
    --db-url "postgresql://user:pass@localhost/datasus"
```

## 📝 Example Results

### Sample Evaluation Output
```
TEXT-TO-SQL METRICS - SAMPLE EVALUATION
============================================================
✅ Loaded 10 questions from ground truth
✅ Initialized 3 metrics

Exact Match (EM):
  Average Score: 0.000
  Accuracy (≥0.8): 0.0% (0/10)
  Perfect Matches: 0/10 (0.0%)

Component Matching (CM):
  Average Score: 0.650
  Accuracy (≥0.8): 0.0% (0/10)
  Perfect Matches: 0/10 (0.0%)

Execution Accuracy (EX): SKIPPED (no database)
```

### Individual Question Analysis
```
GT001: Quantas internações foram registradas no total?
  GT:   SELECT COUNT(*) AS total_internacoes FROM internacoes;
  Pred: SELECT COUNT(*) FROM internacoes;
  Exact Match (EM): 0.000 ❌  (missing alias)
  Component Matching (CM): 0.750 ❌  (partial credit for correct structure)
```

## 🔧 Configuration

### Metric Configuration

```python
# Customize component weights for CM metric
cm_metric = ComponentMatchingMetric()
cm_metric.clause_weights = {
    'select': 0.30,    # Increase SELECT importance
    'from': 0.25,      # Table selection
    'where': 0.25,     # Filtering conditions
    'joins': 0.10,     # Join operations
    'group_by': 0.05,  # Aggregation
    'order_by': 0.03,  # Ordering
    'having': 0.02,    # Post-aggregation filtering
}

# Configure execution timeout for EX metric
ex_metric = ExecutionAccuracyMetric(execution_timeout=60)  # 60 seconds
```

### Database Configuration

```python
from src.infrastructure.database.connection_service import DatabaseConnectionFactory

# PostgreSQL connection
db_connection = DatabaseConnectionFactory.create_postgresql_service(
    "postgresql://username:password@localhost:5432/datasus"
)

# Use with evaluator
evaluator = TextToSQLEvaluator(
    database_connection=db_connection,
    metrics=[ExactMatchMetric(), ComponentMatchingMetric(), ExecutionAccuracyMetric()]
)
```

## 🧪 Testing

### Run Comprehensive Tests

```bash
source .venv/bin/activate
python evaluation/test_metrics.py
```

### Test Coverage
- SQL normalization and parsing
- All three metrics with various SQL patterns
- Error handling and edge cases
- Integration between metrics
- Evaluation orchestrator functionality

## 📊 Integration with Agent

### Automatic Evaluation

```python
from evaluation.evaluator import TextToSQLEvaluator
from src.agent.orchestrator import LangGraphOrchestrator

# Setup
evaluator = TextToSQLEvaluator(database_connection=db_connection)
agent = LangGraphOrchestrator()

# Run evaluation
results = evaluator.evaluate_sample(
    ground_truth_path="evaluation/ground_truth.json",
    agent_orchestrator=agent,
    sample_size=10,
    difficulty_filter=["easy", "medium"]
)

# Results automatically include:
# - Agent success rates
# - Metric scores and accuracy
# - Difficulty-based breakdown
# - Individual question analysis
```

## 📈 Metric Interpretation

### Score Interpretation

| Metric | Score | Interpretation |
|--------|-------|----------------|
| **EM** | 1.0 | Perfect syntactic match |
| **EM** | 0.0 | No syntactic match |
| **CM** | 0.8+ | Query considered correct |
| **CM** | 0.5-0.8 | Partially correct |
| **CM** | <0.5 | Mostly incorrect |
| **EX** | 1.0 | Semantically correct |
| **EX** | 0.0 | Semantically incorrect or execution failed |

### When to Use Each Metric

- **EM**: Strict evaluation for code generation quality
- **CM**: Balanced evaluation allowing structural flexibility
- **EX**: Ultimate test of semantic correctness
- **Combined**: Most comprehensive evaluation approach

## 🛠️ Advanced Features

### Custom Metrics

```python
from evaluation.metrics.base_metrics import BaseMetric, MetricResult

class CustomMetric(BaseMetric):
    def __init__(self):
        super().__init__("Custom Metric")

    def evaluate(self, context: EvaluationContext) -> MetricResult:
        # Custom evaluation logic
        return self._create_result(
            score=calculated_score,
            is_correct=score > threshold,
            details={"custom_info": "value"}
        )
```

### Batch Evaluation

```python
# Evaluate multiple predictions at once
results = []
for question_data in question_batch:
    context = EvaluationContext(...)
    result = evaluator.evaluate_single_prediction(...)
    results.append(result)
```

### Export Results

```python
# Save detailed results
evaluator.save_results(results, "results/detailed_evaluation.json")

# Generate reports
from evaluation.runners.evaluation_runner import generate_report
generate_report(results, output_path, logger)
```

## 🔍 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure project root is in Python path
2. **Database Connection**: Verify PostgreSQL connection string
3. **Memory Issues**: Use smaller sample sizes for large evaluations
4. **SQL Parsing**: Complex queries may need manual review

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed metric analysis
result = metric.evaluate(context)
print(result.details)  # Detailed breakdown
```

## 📚 References

- [Spider: A Large-Scale Human-Labeled Dataset for Complex and Cross-Domain Semantic Parsing and Text-to-SQL Task](https://yale-lily.github.io/spider)
- Yu, T., Zhang, R., Yang, K., Yasunaga, M., Wang, D., Li, Z., ... & Radev, D. (2018). Spider: A large-scale human-labeled dataset for complex and cross-domain semantic parsing and text-to-sql task. arXiv preprint arXiv:1809.08887.

## 🤝 Contributing

1. Add new metrics by extending `BaseMetric`
2. Add tests in `test_metrics.py`
3. Update documentation for new features
4. Follow existing code patterns and conventions

---

**Status**: ✅ Complete and Production Ready
**Last Updated**: September 2025
**Author**: Claude Code Implementation