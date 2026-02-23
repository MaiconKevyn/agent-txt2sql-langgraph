# Direct LLM SQL Baseline (No LangGraph)

This baseline isolates SQL generation to a single direct LLM call:

1. Input question + schema context
2. LLM generates SQL
3. Safety validation (`SELECT` only)
4. SQL execution on PostgreSQL
5. Optional evaluation with EM/CM/EX metrics

No LangGraph state machine or tool-calling flow is used.

## Directory Layout

- `config.py`: runtime configuration from env/CLI
- `context_loader.py`: schema context builder from `TABLE_DESCRIPTIONS`
- `prompt_builder.py`: direct prompt template
- `llm_client.py`: single-shot LLM invocation
- `sql_parser.py`: SQL extraction + safety validation
- `query_executor.py`: PostgreSQL execution wrapper
- `pipeline.py`: orchestration and metrics aggregation
- `run_single.py`: run one question
- `run_batch.py`: run many questions from ground truth

## Environment

Expected variables (or CLI args):

- `DATABASE_URL` (or `BASELINE_DATABASE_URL`)
- `BASELINE_LLM_PROVIDER` (`ollama`, `openai`, `groq`)
- `BASELINE_LLM_MODEL`
- `BASELINE_LLM_TEMPERATURE` (default `0`)
- `BASELINE_LLM_TIMEOUT` (default `120`)
- `BASELINE_STATEMENT_TIMEOUT_MS` (default `60000`)

## Usage

Single question:

```bash
python -m baselines.llm_direct_sql.run_single \
  --question "Quantas internações foram registradas no total?"
```

Batch evaluation against `evaluation/ground_truth.json`:

```bash
python -m baselines.llm_direct_sql.run_batch \
  --max-questions 10
```

Filter by difficulty:

```bash
python -m baselines.llm_direct_sql.run_batch \
  --difficulty easy \
  --difficulty medium
```

