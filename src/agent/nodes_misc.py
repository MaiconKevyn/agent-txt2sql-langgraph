"""Miscellaneous passthrough nodes (reasoning, clarification)."""

import time

from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    add_ai_message,
    update_phase,
)


def reasoning_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """No-op reasoning step to satisfy workflow edges."""
    start = time.time()
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
