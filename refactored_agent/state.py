"""
State definitions for LangGraph Chat Agent.
"""

from typing import TypedDict, List, Dict, Any, Annotated
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.graph.message import add_messages


class ChatState(TypedDict):
    """State for the chat agent workflow."""
    messages: Annotated[List[BaseMessage], add_messages]
    user_query: str
    route: str
    available_tables: List[str]
    selected_tables: List[str]
    schema_context: str
    generated_sql: str
    validated_sql: str
    results: List[Dict[str, Any]]
    response: str
    error: str
    retry_count: int
    max_retries: int
    sql_provided: bool


def make_turn_input(user_query: str, max_retries: int = 2) -> ChatState:
    """Create initial state for a new turn."""
    return {
        "messages": [HumanMessage(content=user_query)],
        "user_query": user_query,
        "route": "",
        "available_tables": [],
        "selected_tables": [],
        "schema_context": "",
        "generated_sql": "",
        "validated_sql": "",
        "results": [],
        "response": "",
        "error": "",
        "retry_count": 0,
        "max_retries": max_retries,
        "sql_provided": False,
    }
