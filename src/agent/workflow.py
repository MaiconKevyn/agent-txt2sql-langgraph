from typing import Literal
from langgraph.graph import StateGraph, START, END

from .state import (
    MessagesStateTXT2SQL,
    QueryRoute,
    ExecutionPhase,
    should_retry
)
from .nodes import (
    query_classification_node,
    list_tables_node,
    get_schema_node,
    generate_sql_node,
    reflect_on_sql_node,
    repair_sql_node,
    validate_sql_node,
    execute_sql_node,
    generate_response_node
)


def create_langgraph_sql_workflow():
    """
    Create LangGraph SQL workflow following official tutorial patterns
    
    This implements the exact structure recommended in the LangGraph SQL Agent tutorial:
    - MessagesState as primary state
    - Tool-based conditional routing
    - Proper error handling and retries
    - Memory checkpointing support
    
    Workflow Structure:
    START → classify → [route based on classification]
    
    Routes:
    1. DATABASE: classify → list_tables → get_schema → generate_sql → validate_sql → execute_sql → response
    2. CONVERSATIONAL: classify → response (direct)
    3. SCHEMA: classify → list_tables → response
    
    With retry mechanisms and error handling at each step.
    """
    
    # Create StateGraph with MessagesState
    workflow = StateGraph(MessagesStateTXT2SQL)
    
    # Add all nodes
    workflow.add_node("classify_query", query_classification_node)
    workflow.add_node("list_tables", list_tables_node)
    workflow.add_node("get_schema", get_schema_node)
    workflow.add_node("generate_sql", generate_sql_node)
    workflow.add_node("reflect_on_sql", reflect_on_sql_node)  # Tier 2: Self-reflection
    workflow.add_node("repair_sql", repair_sql_node)
    workflow.add_node("validate_sql", validate_sql_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("generate_response", generate_response_node)
    
    # Entry point
    workflow.add_edge(START, "classify_query")
    
    # Classification routing (official LangGraph pattern)
    workflow.add_conditional_edges(
        "classify_query",
        route_after_classification,
        {
            "database": "list_tables",
            "conversational": "generate_response",
            "schema": "list_tables",
            "error": "generate_response"
        }
    )
    
    # Database workflow path
    workflow.add_edge("list_tables", "get_schema")
    workflow.add_edge("get_schema", "generate_sql")
    workflow.add_edge("reflect_on_sql", "validate_sql")  # After reflection, go to validation
    workflow.add_edge("repair_sql", "reflect_on_sql")    # After repair, reflect again

    # SQL generation with retry (LangGraph pattern)
    workflow.add_conditional_edges(
        "generate_sql",
        route_after_sql_generation,
        {
            "validate": "reflect_on_sql",  # Changed: reflect before validating
            "retry": "generate_sql",
            "error": "generate_response"
        }
    )
    
    # SQL validation with retry (LangGraph pattern)
    workflow.add_conditional_edges(
        "validate_sql",
        route_after_sql_validation,
        {
            "execute": "execute_sql",
            "retry_generation": "generate_sql",
            "retry_validation": "validate_sql",
            "error": "generate_response"
        }
    )
    
    # SQL execution with retry (LangGraph pattern)
    workflow.add_conditional_edges(
        "execute_sql",
        route_after_sql_execution,
        {
            "response": "generate_response",
            "retry_generation": "repair_sql",
            "retry_validation": "validate_sql", 
            "retry_execution": "execute_sql",
            "error": "generate_response"
        }
    )
    
    # Final response
    workflow.add_edge("generate_response", END)
    
    return workflow


# =============================================================================
# ROUTING FUNCTIONS - Official LangGraph Patterns
# =============================================================================

def route_after_classification(
    state: MessagesStateTXT2SQL
) -> Literal["database", "conversational", "schema", "error"]:
    """
    Route after query classification following LangGraph patterns
    
    This implements the routing logic recommended in the LangGraph SQL Agent tutorial
    """
    
    # Check for classification errors
    if state.get("current_error") or not state.get("query_route"):
        return "error"
    
    query_route = state["query_route"]
    classification = state.get("classification")
    
    # Route based on classification
    if query_route == QueryRoute.CONVERSATIONAL:
        # High-confidence conversational queries go direct to response
        if classification and classification.confidence_score >= 0.7:
            return "conversational"
        else:
            # Low confidence - treat as database query for safety
            return "database"
    
    elif query_route == QueryRoute.SCHEMA:
        # Schema queries need table discovery but skip SQL generation
        return "schema"
    
    elif query_route == QueryRoute.DATABASE:
        # Database queries go through full SQL pipeline
        return "database"
    
    else:
        # Unknown route - default to database processing
        return "database"


def route_after_sql_generation(
    state: MessagesStateTXT2SQL
) -> Literal["validate", "retry", "error"]:
    """
    Route after SQL generation with retry logic
    
    Following LangGraph retry patterns from the official tutorial
    """
    
    generated_sql = state.get("generated_sql")
    current_error = state.get("current_error")
    
    # If SQL was generated successfully, proceed to validation
    if generated_sql and not current_error:
        return "validate"
    
    # If generation failed, check retry conditions
    if current_error:
        error_type = "sql_generation_error"
        
        # Check if we should retry (LangGraph pattern)
        if should_retry(state, error_type):
            # Increment retry count
            state["retry_count"] = state.get("retry_count", 0) + 1
            return "retry"
    
    # Max retries reached or non-retryable error
    return "error"


def route_after_sql_validation(
    state: MessagesStateTXT2SQL
) -> Literal["execute", "retry_generation", "retry_validation", "error"]:
    """
    Route after SQL validation with comprehensive retry logic
    
    Following LangGraph validation patterns from the official tutorial
    """
    
    validated_sql = state.get("validated_sql")
    current_error = state.get("current_error")
    
    # If validation passed, proceed to execution
    if validated_sql and not current_error:
        return "execute"
    
    # If validation failed, determine retry strategy
    if current_error:
        error_type = "sql_validation_error"
        
        # Check if we should retry validation
        if should_retry(state, error_type):
            state["retry_count"] = state.get("retry_count", 0) + 1
            
            # Determine retry type based on error
            error_message = current_error.lower()
            
            if any(keyword in error_message for keyword in ["syntax", "parse", "invalid"]):
                # Syntax errors need SQL regeneration
                return "retry_generation"
            elif any(keyword in error_message for keyword in ["table", "column", "field"]):
                # Schema errors need SQL regeneration
                return "retry_generation"
            else:
                # Other validation errors - retry validation
                return "retry_validation"
    
    # Max retries reached or non-retryable error
    return "error"


def route_after_sql_execution(
    state: MessagesStateTXT2SQL
) -> Literal["response", "retry_generation", "retry_validation", "retry_execution", "error"]:
    """
    Route after SQL execution with comprehensive retry logic
    
    Following LangGraph execution patterns from the official tutorial
    """
    
    sql_execution_result = state.get("sql_execution_result")
    
    # If execution succeeded, proceed to response generation
    if sql_execution_result and sql_execution_result.success:
        return "response"
    
    # If execution failed, determine retry strategy
    current_error = state.get("current_error")
    
    if current_error:
        error_type = "sql_execution_error"
        
        # Check if we should retry
        if should_retry(state, error_type):
            state["retry_count"] = state.get("retry_count", 0) + 1
            
            # Determine retry type based on error
            error_message = current_error.lower()
            
            if any(keyword in error_message for keyword in ["syntax", "parse", "invalid sql"]):
                # SQL syntax errors need regeneration
                return "retry_generation"
            elif any(keyword in error_message for keyword in [
                "table",
                "column",
                "not found",
                "no such",
                "undefined function",
                "função",
                "operator does not exist",
                "cannot cast",
                "type mismatch"
            ]):
                # Schema errors need regeneration with fresh schema
                return "retry_generation"
            elif any(keyword in error_message for keyword in ["timeout", "connection", "database"]):
                # Infrastructure errors - retry execution
                return "retry_execution"
            elif any(keyword in error_message for keyword in ["constraint", "validation"]):
                # Validation errors - retry validation
                return "retry_validation"
            else:
                # Generic errors - retry execution
                return "retry_execution"
    
    # Max retries reached or non-retryable error
    return "error"


# =============================================================================
# WORKFLOW FACTORY FUNCTIONS - Official LangGraph Patterns
# =============================================================================

def create_sql_agent_workflow():
    """Create SQL Agent workflow following official LangGraph tutorial"""
    workflow = create_langgraph_sql_workflow()
    return workflow.compile()


def create_production_sql_agent():
    """Create production-ready SQL agent workflow"""
    return create_sql_agent_workflow()


def create_development_sql_agent():
    """Create development SQL agent workflow with debugging"""
    return create_sql_agent_workflow()


def create_testing_sql_agent():
    """Create testing SQL agent workflow"""
    return create_sql_agent_workflow()


# =============================================================================
# WORKFLOW EXECUTION HELPERS
# =============================================================================

def execute_sql_workflow(
    workflow,
    user_query: str,
    session_id: str = None,
    config: dict = None
) -> dict:
    """
    Execute SQL workflow with proper error handling
    
    Args:
        workflow: Compiled LangGraph workflow
        user_query: User's natural language question
        session_id: Session identifier for checkpointing
        config: Additional configuration
        
    Returns:
        Execution result dictionary
    """
    
    try:
        # Import here to avoid circular dependencies
        from .state import create_initial_messages_state, state_to_legacy_format
        
        # Create initial state
        if session_id is None:
            session_id = f"session_{hash(user_query) % 10000}"
        
        initial_state = create_initial_messages_state(
            user_query=user_query,
            session_id=session_id
        )
        
        config = config or {}

        # Execute workflow
        final_state = workflow.invoke(initial_state, config=config)
        
        # Convert to legacy format for API compatibility
        result = state_to_legacy_format(final_state)
        
        return result
        
    except Exception as e:
        # Handle workflow execution errors
        return {
            "success": False,
            "question": user_query,
            "sql_query": None,
            "results": [],
            "row_count": 0,
            "execution_time": 0.0,
            "error_message": f"Workflow execution failed: {str(e)}",
            "response": f"Erro interno: {str(e)}",
            "timestamp": "2024-01-01T00:00:00",
            "metadata": {
                "langgraph_v3": True,
                "workflow_error": True,
                "error_type": "workflow_execution_error"
            }
        }


def stream_sql_workflow(
    workflow,
    user_query: str,
    session_id: str = None,
    config: dict = None
):
    """
    Stream SQL workflow execution for real-time updates
    
    Args:
        workflow: Compiled LangGraph workflow
        user_query: User's natural language question
        session_id: Session identifier
        config: Additional configuration
        
    Yields:
        State updates during workflow execution
    """
    
    try:
        # Import here to avoid circular dependencies
        from .state import create_initial_messages_state
        
        # Create initial state
        if session_id is None:
            session_id = f"session_{hash(user_query) % 10000}"
        
        initial_state = create_initial_messages_state(
            user_query=user_query,
            session_id=session_id
        )
        
        config = config or {}

        # Stream workflow execution
        for state_update in workflow.stream(initial_state, config=config):
            yield state_update
            
    except Exception as e:
        # Yield error state
        yield {
            "error": f"Workflow streaming failed: {str(e)}",
            "success": False
        }


# Export main factory functions
__all__ = [
    "create_langgraph_sql_workflow",
    "create_sql_agent_workflow", 
    "create_production_sql_agent",
    "create_development_sql_agent",
    "create_testing_sql_agent",
    "execute_sql_workflow",
    "stream_sql_workflow"
]
