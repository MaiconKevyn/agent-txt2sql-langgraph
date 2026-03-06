"""Multi-query executor — generates and executes each sub-query in the plan."""

import time
from typing import Dict, List

from .llm_manager import get_llm_manager
from .sql_generation import build_sql_generation_messages, SQLOutput
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    QueryPlan,
    SubQuery,
    add_ai_message,
    update_phase,
    add_error,
)
from ..utils.logging_config import get_nodes_logger
from ..utils.sql_safety import is_select_only

logger = get_nodes_logger()

MAX_SUB_QUERIES = 4  # hard ceiling — LLM is not allowed to exceed this

_ERROR_INDICATORS = [
    "does not exist",
    "syntax error",
    "invalid sql",
    "error:",
    "psycopg2.errors",
    "relation",
    "column",
    "violates",
]


def _topological_sort(sub_queries: List[SubQuery]) -> List[SubQuery]:
    """Return sub-queries ordered so every dependency comes before its dependent."""
    if not any(sq.depends_on for sq in sub_queries):
        return list(sub_queries)

    id_to_sq = {sq.id: sq for sq in sub_queries}
    visited: set = set()
    ordered: List[SubQuery] = []

    def _visit(sq_id: str) -> None:
        if sq_id in visited:
            return
        visited.add(sq_id)
        sq = id_to_sq.get(sq_id)
        if sq:
            for dep in sq.depends_on:
                _visit(dep)
            ordered.append(sq)

    for sq in sub_queries:
        _visit(sq.id)

    return ordered


def _generate_sql_for_subquery(
    sq: SubQuery,
    user_query: str,
    schema_context: str,
    selected_tables: List[str],
    prior_results: Dict[str, str],
    llm_manager,
) -> str:
    """
    Generate SQL for a single sub-query using the full RULES A-O prompt.

    Uses the shared build_sql_generation_messages helper so that the same
    RULES A-O, table-specific templates, and pre-generation hints apply
    to sub-queries as to single-query generation.
    """
    # Build the sub-query question with prior-results context appended
    subquery_question = sq.description
    if prior_results:
        prior_ctx = "\n[CONTEXT FROM PRIOR QUERIES]\n"
        for dep_id, result_text in prior_results.items():
            prior_ctx += f"  {dep_id}: {result_text[:500]}\n"
        subquery_question = subquery_question + prior_ctx

    # Full RULES A-O prompt (same as generate_sql_node)
    formatted_messages, _ = build_sql_generation_messages(
        user_query=subquery_question,
        schema_context=schema_context,
        selected_tables=selected_tables,
    )

    # Try structured output first (same as generate_sql_node)
    try:
        result = llm_manager.invoke_chat_structured(formatted_messages, SQLOutput)
        return llm_manager._clean_sql_query(result.sql)
    except Exception:
        response = llm_manager.invoke_chat(formatted_messages)
        raw = response.content.strip() if hasattr(response, "content") else str(response)
        return llm_manager._clean_sql_query(raw)


def multi_sql_executor_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    For each sub-query in the plan:
      1. Generate SQL (LLM + schema context, with prior results as context for dependencies)
      2. Safety-check the SQL
      3. Execute against the database via sql_db_query tool
      4. Store result in state["sub_query_results"]

    Sequential dependencies are respected via topological sort.
    """
    start_time = time.time()

    try:
        llm_manager = get_llm_manager()
        query_plan: QueryPlan = state.get("query_plan")

        if not query_plan:
            raise ValueError("No query_plan in state — cannot execute multi-query.")

        user_query = state.get("user_query", "")
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])

        tools = llm_manager.get_sql_tools()
        query_tool = next((t for t in tools if t.name == "sql_db_query"), None)
        if not query_tool:
            raise ValueError("sql_db_query tool not found in LLM manager.")

        # Hard cap: truncate if LLM returned more than MAX_SUB_QUERIES
        if len(query_plan.sub_queries) > MAX_SUB_QUERIES:
            logger.warning(
                "LLM returned too many sub-queries; truncating to %d",
                MAX_SUB_QUERIES,
                extra={"original_count": len(query_plan.sub_queries)},
            )
            query_plan.sub_queries = query_plan.sub_queries[:MAX_SUB_QUERIES]

        ordered_sqs = _topological_sort(query_plan.sub_queries)
        sub_query_results = []
        completed: Dict[str, str] = {}  # id → raw result text for dependency passing

        for sq in ordered_sqs:
            sq_start = time.time()

            # Collect results from dependencies
            prior_results = {dep: completed[dep] for dep in sq.depends_on if dep in completed}

            # --- SQL Generation ---
            try:
                sql = _generate_sql_for_subquery(
                    sq, user_query, schema_context, selected_tables, prior_results, llm_manager
                )
                sq.sql = sql
            except Exception as gen_err:
                sq.success = False
                sq.error = f"SQL generation failed: {gen_err}"
                sub_query_results.append(_make_result(sq, time.time() - sq_start))
                logger.error("Sub-query SQL generation failed", extra={
                    "sq_id": sq.id, "error": str(gen_err)
                })
                continue

            # --- Safety check ---
            ok, reason = is_select_only(sql)
            if not ok:
                sq.success = False
                sq.error = f"Blocked non-SELECT SQL: {reason}"
                sub_query_results.append(_make_result(sq, time.time() - sq_start))
                logger.warning("Sub-query blocked", extra={"sq_id": sq.id, "reason": reason})
                continue

            # --- Execution ---
            try:
                tool_result = query_tool.invoke(sql)
                result_str = str(tool_result).strip()

                lower_result = result_str.lower()
                is_error = any(ind in lower_result for ind in _ERROR_INDICATORS)

                if is_error:
                    sq.success = False
                    sq.error = result_str
                else:
                    sq.success = True
                    sq.result_raw = result_str
                    completed[sq.id] = result_str

            except Exception as exec_err:
                sq.success = False
                sq.error = f"Execution failed: {exec_err}"

            sub_query_results.append(_make_result(sq, time.time() - sq_start))
            logger.info("Sub-query executed", extra={
                "sq_id": sq.id,
                "success": sq.success,
                "sql_preview": (sq.sql or "")[:150],
            })

        state["sub_query_results"] = sub_query_results
        state["query_plan"] = query_plan  # updated with .sql / .result_raw on each SubQuery

        successful = sum(1 for r in sub_query_results if r["success"])
        total = len(sub_query_results)
        state = add_ai_message(
            state,
            f"Multi-query execution complete: {successful}/{total} sub-queries succeeded."
        )

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
        return state

    except Exception as e:
        error_msg = f"Multi-query executor failed: {str(e)}"
        logger.error("multi_sql_executor_node error", extra={"error": str(e)})
        state = add_error(state, error_msg, "sql_execution_error", ExecutionPhase.SQL_EXECUTION)
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
        return state


def _make_result(sq: SubQuery, elapsed: float) -> dict:
    return {
        "id": sq.id,
        "description": sq.description,
        "sql": sq.sql,
        "result": sq.result_raw,
        "success": sq.success,
        "error": sq.error,
        "execution_time": elapsed,
    }
