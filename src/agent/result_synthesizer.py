"""Result synthesizer — combines multi-query results into a final natural language response."""

import time
from typing import List, Dict, Any

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

        # Build context block
        results_text = ""
        any_success = False
        for r in sub_query_results:
            results_text += f"\n--- Sub-consulta [{r['id']}]: {r['description']} ---\n"
            if r.get("sql"):
                results_text += f"SQL executada: {r['sql']}\n"
            if r.get("success") and r.get("result"):
                results_text += f"Resultado:\n{r['result']}\n"
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
