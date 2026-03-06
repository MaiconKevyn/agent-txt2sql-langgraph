"""
nodes.py — thin re-export facade.

All logic has been moved to focused sub-modules for maintainability.
This module re-exports every public symbol so that workflow.py and
orchestrator.py continue to work without modification.

Pipeline execution order (see workflow.py for the full graph):
  1. classification.py   — classifica a query (DATABASE / CONVERSATIONAL / SCHEMA)
  2. table_selection.py  — descobre e seleciona tabelas (heurística → embedding → LLM)
  3. schema_node.py      — busca o schema das tabelas selecionadas (com cache)
  4. sql_generation.py   — gera o SQL (RULES A-H + few-shots + pre-generation hints)
  5. validation.py       — valida o SQL (DB EXPLAIN + checks semânticos)
  6. execution.py        — executa o SQL; em erro, repair_sql_node corrige e re-executa
  7. response.py         — formata a resposta final em português natural

Utilitários compartilhados (não são nós do pipeline):
  schema_utils.py        — parsing de schema, verificação de colunas, sugestões
  table_selector.py      — EmbeddingTableSelector (Stage 2 da seleção de tabelas)
  llm_manager.py         — OpenAILLMManager + singleton get/set_global_llm_manager
  nodes_misc.py          — reasoning_node e clarification_node (passthrough stubs)
"""

# LLM manager singleton (used by orchestrator)
from .llm_manager import get_llm_manager, set_global_llm_manager, OpenAILLMManager  # noqa: F401

# Node functions — in pipeline order
from .classification import query_classification_node  # noqa: F401  # step 1
from .table_selection import list_tables_node          # noqa: F401  # step 2
from .schema_node import get_schema_node               # noqa: F401  # step 3
from .sql_generation import generate_sql_node, SQLOutput  # noqa: F401  # step 4
from .validation import validate_sql_node              # noqa: F401  # step 5
from .execution import execute_sql_node, repair_sql_node  # noqa: F401  # step 6
from .response import generate_response_node           # noqa: F401  # step 7
from .nodes_misc import reasoning_node, clarification_node  # noqa: F401
from .vote_sql import vote_sql_node                    # noqa: F401  # step 4b (voting)
from .plan_gate import plan_gate_node                 # noqa: F401

# Internal helpers exposed for backward compatibility (tests, evaluation scripts)
from .schema_utils import (  # noqa: F401
    _parse_schema_columns,
    _extract_alias_map,
    _extract_alias_columns,
    _best_column_suggestions,
    _check_columns_against_schema,
)
from .schema_node import (  # noqa: F401
    _schema_cache,
    _should_refresh_schema,
    _refresh_schema_context,
    _enhance_sus_schema_context,
)
from .table_selection import (  # noqa: F401
    _heuristic_table_selection,
    _select_relevant_tables,
    _parse_llm_table_selection,
    _validate_table_selection,
    _get_intelligent_fallback,
)
from .sql_generation import _build_pregeneration_hints  # noqa: F401
from .response import _generate_formatted_response, _generate_fallback_response  # noqa: F401
from .query_planner import query_planner_node  # noqa: F401
from .multi_executor import multi_sql_executor_node  # noqa: F401
from .multi_verifier import multi_verifier_node  # noqa: F401
from .result_synthesizer import result_synthesizer_node  # noqa: F401

__all__ = [
    "query_classification_node",
    "list_tables_node",
    "get_schema_node",
    "reasoning_node",
    "generate_sql_node",
    "repair_sql_node",
    "validate_sql_node",
    "execute_sql_node",
    "generate_response_node",
    "clarification_node",
    "vote_sql_node",
    "plan_gate_node",
    "get_llm_manager",
    "set_global_llm_manager",
    "query_planner_node",
    "multi_sql_executor_node",
    "multi_verifier_node",
    "result_synthesizer_node",
]
