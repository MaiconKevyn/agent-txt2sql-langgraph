# Evaluation Archive

This directory contains development artifacts, demos, and old evaluation results that were moved during cleanup (2025-10-08).

## 📦 Archived Content

### Demo Files
- `demo_complete_system.py` - Complete system demonstration script
- `demo_execution_accuracy.py` - Execution accuracy metric demo
- `final_demonstration.py` - Final demonstration script
- `comprehensive_analysis.py` - Comprehensive analysis tools

### Specialized Evaluators
- `database_evaluator.py` - Database-enabled evaluator (functionality merged into main evaluator.py)
- `test_improved_parser.py` - SQL parser development tests

### Old Results
Located in `results_old/`:
- `database_evaluation_test.json` - Database evaluation test (Sep 29)
- `improved_evaluation_test.json` - Improved evaluation test (Sep 29)
- `tables_result_13.json` through `tables_result_16_outputs.json` - Development iterations (Sep 20-21)

## 🎯 Why Archived?

These files were moved to simplify the main evaluation structure:

1. **Demos** - Useful for development/learning but not needed for production evaluation
2. **Specialized evaluators** - Functionality consolidated into main `evaluator.py`
3. **Old results** - Historical data from development iterations

## ♻️ If You Need These Files

All files remain available in this archive and can be restored if needed:

```bash
# Restore a demo
cp archive/demo_complete_system.py ../

# Restore old results for comparison
cp archive/results_old/database_evaluation_test.json ../results/
```

## 📊 Current Production Structure

The main evaluation directory now contains only essential files:

```
evaluation/
├── metrics/              # Core metric implementations (EM, CM, EX)
├── runners/              # CLI evaluation runner
├── evaluator.py          # Main orchestrator
├── ground_truth.json     # Ground truth dataset
├── run_sample_evaluation.py  # Quick test script
├── test_metrics.py       # Unit tests
└── README.md             # Documentation
```

---

**Archived**: October 8, 2025
**Reason**: Code cleanup and organization
**Status**: Safe to keep or delete
