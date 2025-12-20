# Refactored TXT2SQL Agent

This is a simplified, self-contained refactor of the TXT2SQL agent using
LangGraph for chat memory and retry orchestration.

## Key changes
- LangGraph workflow with in-memory chat history (MemorySaver)
- Single pipeline (classification -> selection -> schema -> SQL -> validation -> execute -> response)
- Heuristic classification to reduce LLM calls
- Deterministic response formatting (no LLM for formatting)
- Direct DB access with SQLAlchemy
- One optional repair attempt on SQL validation/execution failure

## Usage

Dry run (no DB needed):

```bash
python -m refactored_agent.cli --dry-run
```

Self test (requires DB URL):

```bash
python -m refactored_agent.cli --db-url "postgresql+psycopg2://user:pass@host:5432/db" --self-test
```

Run a query:

```bash
python -m refactored_agent.cli --db-url "postgresql+psycopg2://user:pass@host:5432/db" --query "Quantos obitos ocorreram?"
```

Interactive chat (memory per session id):

```bash
python -m refactored_agent.cli --db-url "postgresql+psycopg2://user:pass@host:5432/db" --interactive --session-id chat1
```

Environment variables supported:
- DATABASE_URL / DATABASE_PATH
- LLM_PROVIDER (default: ollama)
- LLM_MODEL (default: llama3.1:8b)
- LLM_TEMPERATURE (default: 0.1)
- LLM_TIMEOUT (default: 120)
