"""Miscellaneous passthrough nodes (reasoning, clarification)."""

import time

from langchain_core.messages import HumanMessage, SystemMessage

from .llm_manager import get_llm_manager
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    add_ai_message,
    update_phase,
)
from ..utils.logging_config import get_nodes_logger

logger = get_nodes_logger()

_COT_SYSTEM_PROMPT = """\
Você é um especialista em SQL PostgreSQL para dados de saúde pública do DATASUS (SIH-RS).

Analise a pergunta do usuário e produza um PLANO SQL ESTRUTURADO em até 8 linhas para guiar a geração.
Indique:
1. Tabelas e colunas principais necessárias
2. Padrão SQL obrigatório (escolha um): CTE com média global → filtro local | ROW_NUMBER OVER PARTITION BY | CASE WHEN pivot colunas | NOT EXISTS anti-join | dois períodos em CTEs separadas + delta absoluto | subquery simples
3. Filtros e condições de escopo (HAVING, WHERE com threshold, filtros de valor)
4. Uma armadilha específica a evitar para esta pergunta

Seja direto e técnico. NÃO escreva SQL — apenas o plano textual.
"""


def reasoning_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """CoT SQL planning: generate a structured SQL sketch before generation."""
    start = time.time()

    user_query = state.get("user_query", "")
    plan_type = state.get("plan_type", "single_default")
    selected_tables = state.get("selected_tables", [])

    try:
        llm_manager = get_llm_manager()
        human_prompt = (
            f"Pergunta: {user_query}\n\n"
            f"Tipo de consulta detectado: {plan_type}\n"
            f"Tabelas selecionadas: {', '.join(selected_tables) if selected_tables else 'a determinar'}\n\n"
            "Produza o plano SQL estruturado:"
        )
        response = llm_manager.invoke_chat([
            SystemMessage(content=_COT_SYSTEM_PROMPT),
            HumanMessage(content=human_prompt),
        ])
        reasoning_plan = response.content.strip() if hasattr(response, "content") else str(response)
        state["reasoning_plan"] = reasoning_plan
        logger.info("CoT reasoning plan generated", extra={
            "plan_type": plan_type,
            "plan_length": len(reasoning_plan),
        })
    except Exception as e:
        logger.warning("reasoning_node CoT failed — continuing without plan", extra={"error": str(e)})
        state["reasoning_plan"] = None

    state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start)
    return state


def clarification_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """Fallback clarification response if routed here."""
    prompt = (
        "Não consegui entender totalmente sua pergunta. "
        "Por favor, reformule adicionando contexto (tabelas, filtros, período)."
    )
    state = add_ai_message(state, prompt)
    state["final_response"] = prompt
    state["completed"] = True
    state = update_phase(state, ExecutionPhase.RESPONSE_FORMATTING, 0.0)
    return state
