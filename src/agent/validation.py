"""SQL validation node."""

import re
import time

from .llm_manager import get_llm_manager
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    ToolCallResult,
    add_ai_message,
    add_tool_call_result,
    update_phase,
    add_error,
)
from ..utils.logging_config import get_nodes_logger

logger = get_nodes_logger()


def validate_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Validate SQL Node - Using SQLDatabaseToolkit query checker

    Uses the sql_db_query_checker tool from SQLDatabaseToolkit
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()

    try:
        llm_manager = get_llm_manager()
        generated_sql = state.get("generated_sql")

        if not generated_sql:
            raise ValueError("No SQL query to validate")

        tools = llm_manager.get_sql_tools()
        checker_tool = next((tool for tool in tools if tool.name == "sql_db_query_checker"), None)

        validation_passed = True
        validation_message = "SQL query is valid"
        checker_msg = None

        # LLM-based checker
        if checker_tool:
            try:
                checker_result = checker_tool.invoke(generated_sql)
                checker_msg = str(checker_result)
                if "error" in checker_msg.lower() or "invalid" in checker_msg.lower():
                    validation_passed = False
                    validation_message = checker_msg
            except Exception as checker_error:
                validation_passed = False
                validation_message = f"Query checker failed: {str(checker_error)}"

        # DB EXPLAIN — takes precedence
        db_val = llm_manager.validate_sql_query(generated_sql)
        if not db_val.get("is_valid", False):
            validation_passed = False
            validation_message = db_val.get("error", "DB validation failed")
        elif validation_passed is False and db_val.get("is_valid", False):
            validation_passed = True
            validation_message = "DB validation passed"

        # Semantic: socioeconomico requires metrica filter
        if validation_passed and generated_sql:
            sql_lower = generated_sql.lower()
            if 'socioeconomico' in sql_lower and 'metrica' not in sql_lower:
                validation_passed = False
                validation_message = (
                    "SEMANTIC ERROR: Query uses 'socioeconomico' table but is missing a "
                    "WHERE metrica = '...' filter. The socioeconomico table is long-format "
                    "(one row per metric × municipality × year). Without a metrica filter, "
                    "the query aggregates across ALL metrics (population, IDHM, bolsa familia, "
                    "etc.) which produces meaningless results. "
                    "FIX EXACTLY: "
                    "SELECT mu.nome AS municipio_maior_populacao "
                    "FROM socioeconomico s "
                    "JOIN municipios mu ON s.codigo_6d = mu.codigo_6d "
                    "WHERE s.metrica = 'populacao_total' "
                    "ORDER BY s.valor DESC LIMIT 1; "
                    "IMPORTANT: SELECT ONLY the column(s) that directly answer the question. "
                    "Do NOT include s.valor or extra columns unless explicitly requested."
                )
                logger.warning("Socioeconomico query rejected: missing metrica filter")

        # Semantic: tempo table cartesian explosion
        if validation_passed and generated_sql:
            if re.search(r'\bjoin\s+tempo\b', generated_sql, re.I):
                has_proper_equijoin = bool(re.search(
                    r'on\s+\w+\."DT_INTER"\s*=\s*\w+\."data"',
                    generated_sql, re.I,
                ))
                if not has_proper_equijoin:
                    validation_passed = False
                    validation_message = (
                        "TEMPO TABLE ERROR: Non-equijoin on tempo table detected "
                        "(e.g., ON EXTRACT(YEAR FROM \"DT_INTER\") = t.ano or ON t.mes BETWEEN ...). "
                        "This creates a CARTESIAN PRODUCT multiplying rows by hundreds or thousands! "
                        "SOLUTION: Remove the JOIN tempo entirely. Use EXTRACT() directly without any JOIN: "
                        "✅ WHERE EXTRACT(YEAR FROM \"DT_INTER\") = 2015 "
                        "✅ WHERE EXTRACT(MONTH FROM \"DT_INTER\") IN (6, 7, 8) "
                        "✅ WHERE EXTRACT(MONTH FROM \"DT_INTER\") BETWEEN 6 AND 8"
                    )
                    logger.warning("Tempo non-equijoin rejected: would cause cartesian explosion")

        # Semantic: spurious VAL_UTI on obstetric query
        if validation_passed and generated_sql:
            espec_2 = bool(re.search(r'"ESPEC"\s*=\s*2\b', generated_sql))
            val_uti = bool(re.search(r'"VAL_UTI"\s*>\s*0', generated_sql))
            if espec_2 and val_uti:
                user_query_lower = (state.get('user_query') or '').lower()
                uti_mentioned = any(k in user_query_lower for k in [
                    'uti', 'unidade de terapia', 'custo de uti', 'valor de uti', 'custo uti', 'icú'
                ])
                if not uti_mentioned:
                    validation_passed = False
                    validation_message = (
                        "ERROR: Added 'VAL_UTI > 0' to an obstetric query when UTI was not mentioned. "
                        "Obstetric = WHERE \"ESPEC\" = 2 ONLY (no UTI filter needed here). "
                        "REMOVE the AND \"VAL_UTI\" > 0 condition from this query. "
                        "Only add VAL_UTI > 0 when the question explicitly asks about UTI/ICU."
                    )
                    logger.warning("Spurious VAL_UTI filter on obstetric query rejected")

        # Semantic: spurious MORTE = false
        if validation_passed and generated_sql:
            has_morte_false = bool(re.search(r'"MORTE"\s*=\s*(false|FALSE)\b', generated_sql))
            if has_morte_false:
                user_query_lower = (state.get('user_query') or '').lower()
                discharge_asked = any(k in user_query_lower for k in [
                    'alta', 'sobrevivente', 'não morreram', 'sem óbito', 'recuper', 'vivos', 'saíram vivos'
                ])
                if not discharge_asked:
                    validation_passed = False
                    validation_message = (
                        "ERROR: Added 'MORTE = false' filter but the question does not ask specifically "
                        "about discharged (non-death) patients. "
                        "REMOVE the '\"MORTE\" = false' condition. "
                        "Count ALL patients matching the other conditions, regardless of outcome."
                    )
                    logger.warning("Spurious MORTE=false filter rejected")

        # Update state
        if validation_passed:
            state["validated_sql"] = generated_sql
            state["current_error"] = None
            ai_response = f"SQL query validated successfully: {generated_sql}"
        else:
            state = add_error(state, validation_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
            state["retry_count"] = state.get("retry_count", 0) + 1
            state["validation_retry_count"] = state.get("validation_retry_count", 0) + 1
            errs = state.get("validation_errors", []) or []
            errs.append(validation_message)
            state["validation_errors"] = errs
            ai_response = f"SQL validation failed: {validation_message}"

        state = add_ai_message(state, ai_response)

        if checker_tool:
            tool_call_result = ToolCallResult(
                tool_name="sql_db_query_checker",
                tool_input={"query": generated_sql},
                tool_output=checker_msg or validation_message,
                success=(checker_msg is None) or (
                    "error" not in (checker_msg or "").lower()
                    and "invalid" not in (checker_msg or "").lower()
                ),
                execution_time=time.time() - start_time,
            )
            state = add_tool_call_result(state, tool_call_result)

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_VALIDATION, execution_time)

        return state

    except Exception as e:
        error_message = f"SQL validation failed: {str(e)}"
        state = add_error(state, error_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
        state["retry_count"] = state.get("retry_count", 0) + 1
        state["validation_retry_count"] = state.get("validation_retry_count", 0) + 1

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_VALIDATION, execution_time)

        return state
