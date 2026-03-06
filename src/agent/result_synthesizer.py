"""Result synthesizer — combines multi-query results into a final natural language response."""

import ast
import time
from typing import List, Dict, Any, Optional

from langchain_core.messages import SystemMessage, HumanMessage

from .llm_manager import get_llm_manager
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    add_ai_message,
    update_phase,
    add_error,
)
from ..utils.logging_config import get_nodes_logger

logger = get_nodes_logger()


def _parse_result_rows(result_raw: str) -> Optional[List]:
    """
    Parse the string returned by LangChain's sql_db_query tool into a list of tuples.

    The tool typically returns strings of the form:
      "[(val1, val2), (val3, val4)]"
    or a single-column result like:
      "[(val1,), (val2,)]"
    or even a plain scalar like "42".

    Returns None if parsing fails (caller falls back to SQL re-execution).
    """
    if not result_raw:
        return []
    text = result_raw.strip()
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            # Normalise each element to a tuple
            rows = []
            for item in parsed:
                if isinstance(item, tuple):
                    rows.append(item)
                else:
                    rows.append((item,))
            return rows
        # Single scalar value
        return [(parsed,)]
    except Exception:
        return None


def _collect_leaf_rows(sub_query_results: List[Dict[str, Any]]) -> Optional[List]:
    """
    Identify leaf sub-queries (not depended upon by any other) and concatenate
    their parsed result rows.  Returns None if parsing fails for any leaf.
    """
    # Find all IDs that appear as a dependency of some other sub-query
    # (sub_query_results are plain dicts from _make_result, not SubQuery objects)
    # We inspect the original SubQuery objects via the plan — but since we only
    # have dicts here, we derive depends_on from what was stored.  The dicts do
    # NOT store depends_on, so we treat ALL successful results as leaves when
    # there is no dependency metadata available (safe fallback: concatenate all).
    successful = [r for r in sub_query_results if r.get("success") and r.get("result")]
    if not successful:
        return None

    all_rows: List = []
    for r in successful:
        rows = _parse_result_rows(r["result"])
        if rows is None:
            return None  # parsing failure — fall back to SQL re-execution
        all_rows.extend(rows)

    return all_rows if all_rows else None


_SYSTEM_PROMPT = """\
Você é um assistente especializado em dados de saúde pública brasileira (DATASUS/SIH-RS).
Sua tarefa: sintetizar os resultados de múltiplas consultas SQL em uma resposta única,
clara e direta em português.

Diretrizes:
- Seja conciso. Apresente os números de forma organizada.
- Se alguma sub-consulta falhou, mencione brevemente.
- Responda APENAS com a resposta final para o usuário — sem explicar estrutura técnica,
  sem mencionar "sub-queries" ou "SQL" na resposta.
"""


def result_synthesizer_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Synthesize results from multiple sub-queries into a single natural language response.
    Sets state["final_response"], state["success"], state["completed"].
    """
    start_time = time.time()

    try:
        llm_manager = get_llm_manager()
        user_query = state.get("user_query", "")
        sub_query_results: List[Dict[str, Any]] = state.get("sub_query_results", [])

        if not sub_query_results:
            state["final_response"] = (
                "Não foi possível executar as consultas necessárias para responder à pergunta."
            )
            state["success"] = False
            state["completed"] = True
            state = update_phase(state, ExecutionPhase.RESPONSE_FORMATTING, time.time() - start_time)
            return state

        # Build context block — cap each result at 3000 chars to avoid context overflow
        _MAX_RESULT_CHARS = 3000
        results_text = ""
        any_success = False
        for r in sub_query_results:
            results_text += f"\n--- Sub-consulta [{r['id']}]: {r['description']} ---\n"
            if r.get("sql"):
                results_text += f"SQL executada: {r['sql']}\n"
            if r.get("success") and r.get("result"):
                raw = r["result"]
                truncated = raw[:_MAX_RESULT_CHARS] + (
                    f"\n... [truncado — {len(raw) - _MAX_RESULT_CHARS} chars omitidos]"
                    if len(raw) > _MAX_RESULT_CHARS else ""
                )
                results_text += f"Resultado:\n{truncated}\n"
                any_success = True
            else:
                results_text += f"Erro: {r.get('error', 'desconhecido')}\n"

        if not any_success:
            state["final_response"] = (
                "Não foi possível obter resultados das consultas. Por favor, reformule a pergunta."
            )
            state["success"] = False
            state["completed"] = True
            state = update_phase(state, ExecutionPhase.RESPONSE_FORMATTING, time.time() - start_time)
            return state

        human_prompt = (
            f"Pergunta original do usuário: {user_query}\n\n"
            f"Resultados das consultas executadas:\n{results_text}\n\n"
            "Sintetize os resultados em uma resposta clara e objetiva para o usuário:"
        )

        response = llm_manager.invoke_chat([
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content=human_prompt),
        ])

        final_response = (
            response.content.strip() if hasattr(response, "content") else str(response)
        )

        state["final_response"] = final_response
        state["success"] = True
        state["completed"] = True

        # Expose first successful SQL for evaluation / logging
        for r in sub_query_results:
            if r.get("success") and r.get("sql"):
                state["generated_sql"] = r["sql"]
                state["validated_sql"] = r["sql"]
                break

        # Store parsed result rows for EX evaluation (avoids re-executing partial SQL)
        state["final_result_rows"] = _collect_leaf_rows(sub_query_results)

        state = add_ai_message(state, final_response)

        logger.info("Result synthesizer complete", extra={
            "n_results": len(sub_query_results),
            "any_success": any_success,
        })

        state = update_phase(state, ExecutionPhase.RESPONSE_FORMATTING, time.time() - start_time)
        return state

    except Exception as e:
        error_msg = f"Result synthesizer failed: {str(e)}"
        logger.error("result_synthesizer_node error", extra={"error": str(e)})
        state = add_error(state, error_msg, "sql_execution_error", ExecutionPhase.RESPONSE_FORMATTING)
        state["completed"] = True
        state["success"] = False
        state = update_phase(state, ExecutionPhase.RESPONSE_FORMATTING, time.time() - start_time)
        return state
