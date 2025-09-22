from typing import TypedDict, Optional, List, Dict, Any, Annotated, Sequence
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, ToolMessage


class QueryRoute(Enum):
    """Query routing options following LangGraph patterns"""
    DATABASE = "database"  # Requires SQL generation and execution
    CONVERSATIONAL = "conversational"  # Direct LLM response
    SCHEMA = "schema"  # Schema introspection
    TOOL_CALL = "tool_call"  # Tool-based response


class ExecutionPhase(Enum):
    """Enhanced execution phases for LangGraph workflow"""
    INITIALIZATION = "initialization"
    QUERY_CLASSIFICATION = "query_classification"
    TABLE_DISCOVERY = "table_discovery"
    SCHEMA_ANALYSIS = "schema_analysis"
    SQL_GENERATION = "sql_generation"
    SQL_VALIDATION = "sql_validation"
    SQL_EXECUTION = "sql_execution"
    SQL_REPAIR = "sql_repair"
    TOOL_EXECUTION = "tool_execution"
    RESULT_INTERPRETATION = "result_interpretation"
    RESPONSE_FORMATTING = "response_formatting"
    ERROR_HANDLING = "error_handling"
    COMPLETED = "completed"


@dataclass
class QueryClassification:
    """Enhanced query classification following LangGraph patterns"""
    route: QueryRoute
    confidence_score: float
    reasoning: str
    requires_tools: bool
    estimated_complexity: float
    suggested_approach: str


@dataclass
class ToolCallResult:
    """Result from tool execution"""
    tool_name: str
    tool_input: Dict[str, Any]
    tool_output: Any
    success: bool
    execution_time: float
    error_message: Optional[str] = None


@dataclass
class SQLExecutionResult:
    """Enhanced SQL execution result"""
    success: bool
    sql_query: str
    results: List[Dict[str, Any]]
    row_count: int
    execution_time: float
    validation_passed: bool
    error_message: Optional[str] = None
    warnings: List[str] = None


class MessagesStateTXT2SQL(TypedDict):
    """
    Primary state following LangGraph MessagesState pattern
    
    This follows the official LangGraph SQL Agent tutorial structure:
    - Messages as the primary conversation state
    - Structured data for workflow management
    - Tool calling support
    - Proper message history tracking
    """
    
    # PRIMARY: Messages state
    messages: Annotated[Sequence[BaseMessage], add_messages]
    
    # Core query information
    user_query: str
    session_id: str
    timestamp: datetime
    
    # Query routing and classification
    query_route: Optional[QueryRoute]
    classification: Optional[QueryClassification]
    requires_sql: bool
    
    # Workflow state
    current_phase: ExecutionPhase
    completed_phases: List[ExecutionPhase]
    
    # Database context
    available_tables: List[str]
    selected_tables: List[str]
    schema_context: str
    
    # SQL processing
    generated_sql: Optional[str]
    validated_sql: Optional[str]
    sql_execution_result: Optional[SQLExecutionResult]
    
    # Tool calling (official LangGraph pattern)
    tool_calls: List[ToolCallResult]
    pending_tool_calls: List[Dict[str, Any]]
    
    # Results and response
    final_response: Optional[str]
    response_metadata: Dict[str, Any]
    
    # Error handling
    errors: List[Dict[str, Any]]
    current_error: Optional[str]
    retry_count: int
    max_retries: int
    
    # Performance tracking
    execution_time_total: float
    phase_timings: Dict[str, float]
    
    # Success tracking
    success: bool
    completed: bool


def create_initial_messages_state(
    user_query: str,
    session_id: str,
    max_retries: int = 3
) -> MessagesStateTXT2SQL:
    """
    Create initial state following LangGraph MessagesState pattern
    
    Args:
        user_query: User's natural language question
        session_id: Unique session identifier
        max_retries: Maximum retry attempts
        
    Returns:
        Initial MessagesState for TXT2SQL workflow
    """
    
    # Create initial human message
    initial_message = HumanMessage(content=user_query)
    
    return MessagesStateTXT2SQL(
        # PRIMARY: Messages state
        messages=[initial_message],
        
        # Core query
        user_query=user_query,
        session_id=session_id,
        timestamp=datetime.now(),
        
        # Query routing
        query_route=None,
        classification=None,
        requires_sql=False,
        
        # Workflow state
        current_phase=ExecutionPhase.INITIALIZATION,
        completed_phases=[],
        
        # Database context
        available_tables=[],
        selected_tables=[],
        schema_context="",
        
        # SQL processing
        generated_sql=None,
        validated_sql=None,
        sql_execution_result=None,
        
        # Tool calling
        tool_calls=[],
        pending_tool_calls=[],
        
        # Results
        final_response=None,
        response_metadata={},
        
        # Error handling
        errors=[],
        current_error=None,
        retry_count=0,
        max_retries=max_retries,
        
        # Performance
        execution_time_total=0.0,
        phase_timings={},
        
        # Status
        success=False,
        completed=False
    )


def add_system_message(
    state: MessagesStateTXT2SQL,
    content: str
) -> MessagesStateTXT2SQL:
    """Add system message to state following LangGraph patterns"""
    system_message = SystemMessage(content=content)
    state["messages"] = add_messages(state["messages"], [system_message])
    return state


def add_ai_message(
    state: MessagesStateTXT2SQL,
    content: str,
    tool_calls: Optional[List[Dict[str, Any]]] = None
) -> MessagesStateTXT2SQL:
    """Add AI message to state following LangGraph patterns"""
    ai_message = AIMessage(content=content)
    
    # Add tool calls if present
    if tool_calls:
        ai_message.tool_calls = tool_calls
    
    state["messages"] = add_messages(state["messages"], [ai_message])
    return state


def add_tool_message(
    state: MessagesStateTXT2SQL,
    tool_call_id: str,
    content: str,
    tool_name: str
) -> MessagesStateTXT2SQL:
    """Add tool message to state following LangGraph patterns"""
    tool_message = ToolMessage(
        content=content,
        tool_call_id=tool_call_id,
        name=tool_name
    )
    state["messages"] = add_messages(state["messages"], [tool_message])
    return state


def update_phase(
    state: MessagesStateTXT2SQL,
    new_phase: ExecutionPhase,
    execution_time: Optional[float] = None
) -> MessagesStateTXT2SQL:
    """Update current phase and track timing"""
    
    # Mark previous phase as completed
    if state["current_phase"] not in state["completed_phases"]:
        state["completed_phases"].append(state["current_phase"])
    
    # Update current phase
    state["current_phase"] = new_phase
    
    # Track timing if provided
    if execution_time is not None:
        phase_name = new_phase.value
        state["phase_timings"][phase_name] = execution_time
        state["execution_time_total"] += execution_time
    
    return state


def add_error(
    state: MessagesStateTXT2SQL,
    error_message: str,
    error_type: str,
    phase: ExecutionPhase
) -> MessagesStateTXT2SQL:
    """Add error to state with context"""
    error_entry = {
        "message": error_message,
        "type": error_type,
        "phase": phase.value,
        "timestamp": datetime.now().isoformat(),
        "retry_count": state["retry_count"]
    }
    
    state["errors"].append(error_entry)
    state["current_error"] = error_message
    
    return state


def add_tool_call_result(
    state: MessagesStateTXT2SQL,
    tool_result: ToolCallResult
) -> MessagesStateTXT2SQL:
    """Add tool call result to state"""
    state["tool_calls"].append(tool_result)
    
    # Add corresponding tool message to messages
    state = add_tool_message(
        state,
        tool_call_id=f"call_{len(state['tool_calls'])}",
        content=str(tool_result.tool_output),
        tool_name=tool_result.tool_name
    )
    
    return state


def should_retry(
    state: MessagesStateTXT2SQL,
    error_type: str
) -> bool:
    """Determine if operation should be retried with smart retry limits"""
    current_retry = state["retry_count"]
    max_retries = state["max_retries"]
    
    # Smart retry limits per error type
    retry_limits = {
        "sql_syntax_error": 2,
        "sql_validation_error": 2, 
        "tool_execution_error": 3,
        "llm_timeout": 1,
        "database_connection_error": 3,
        "classification_error": 1,
        "schema_error": 2
    }
    
    # Get specific limit for this error type
    specific_limit = retry_limits.get(error_type, max_retries)
    
    return current_retry < min(specific_limit, max_retries)


def get_conversation_history(
    state: MessagesStateTXT2SQL,
    include_system: bool = False
) -> List[BaseMessage]:
    """Get conversation history for LLM context"""
    messages = state["messages"]
    
    if not include_system:
        # Filter out system messages
        messages = [msg for msg in messages if not isinstance(msg, SystemMessage)]
    
    return messages


def format_for_llm_input(
    state: MessagesStateTXT2SQL,
    system_prompt: Optional[str] = None
) -> List[BaseMessage]:
    """Format state for LLM input following LangGraph patterns"""
    messages = []
    
    # Add system prompt if provided
    if system_prompt:
        messages.append(SystemMessage(content=system_prompt))
    
    # Add conversation history
    messages.extend(get_conversation_history(state, include_system=False))
    
    return messages


def extract_sql_from_messages(state: MessagesStateTXT2SQL) -> Optional[str]:
    """Extract SQL query from AI messages in conversation"""
    for message in reversed(state["messages"]):
        if isinstance(message, AIMessage):
            content = message.content
            
            # Look for SQL patterns
            if any(keyword in content.lower() for keyword in ["select", "insert", "update", "delete"]):
                # Extract SQL from markdown or raw text
                sql = content.strip()
                
                # Remove markdown formatting
                if "```sql" in sql:
                    sql = sql.split("```sql")[1].split("```")[0].strip()
                elif "```" in sql:
                    sql = sql.split("```")[1].split("```")[0].strip()
                
                return sql
    
    return None


def get_latest_ai_response(state: MessagesStateTXT2SQL) -> Optional[str]:
    """Get the latest AI response from messages"""
    for message in reversed(state["messages"]):
        if isinstance(message, AIMessage):
            return message.content
    return None


def calculate_success_metrics(state: MessagesStateTXT2SQL) -> Dict[str, Any]:
    """Calculate success metrics for the workflow execution"""
    total_phases = len(ExecutionPhase)
    completed_phases = len(state["completed_phases"])
    
    success_rate = completed_phases / total_phases if total_phases > 0 else 0
    
    tool_success_rate = 0
    if state["tool_calls"]:
        successful_tools = sum(1 for tool in state["tool_calls"] if tool.success)
        tool_success_rate = successful_tools / len(state["tool_calls"])
    
    return {
        "overall_success": state["success"],
        "completion_rate": success_rate,
        "phases_completed": completed_phases,
        "total_phases": total_phases,
        "tool_success_rate": tool_success_rate,
        "total_tools_used": len(state["tool_calls"]),
        "retry_count": state["retry_count"],
        "error_count": len(state["errors"]),
        "execution_time": state["execution_time_total"]
    }


def state_to_legacy_format(state: MessagesStateTXT2SQL) -> Dict[str, Any]:
    """
    Convert MessagesState to legacy QueryResult format
    
    Maintains API compatibility during migration
    """
    
    # Extract SQL and results
    sql_query = state.get("validated_sql") or state.get("generated_sql", "")
    results = []
    row_count = 0
    execution_time = state.get("execution_time_total", 0.0)
    
    if state.get("sql_execution_result"):
        exec_result = state["sql_execution_result"]
        results = exec_result.results
        row_count = exec_result.row_count
        execution_time = exec_result.execution_time
    
    # Get response text
    response_text = (
        state.get("final_response") or 
        get_latest_ai_response(state) or 
        ""
    )
    
    # Get success metrics
    metrics = calculate_success_metrics(state)
    
    return {
        "success": state["success"],
        "question": state["user_query"],
        "sql_query": sql_query,
        "results": results,
        "row_count": row_count,
        "execution_time": execution_time,
        "error_message": state.get("current_error"),
        "response": response_text,
        "timestamp": state["timestamp"].isoformat(),
        "metadata": {
            # Enhanced metadata with LangGraph info
            "langgraph_v3": True,
            "messages_state": True,
            "query_route": state.get("query_route", {}).value if state.get("query_route") else None,
            "classification": {
                "route": state.get("classification", {}).route.value if state.get("classification") else None,
                "confidence": state.get("classification", {}).confidence_score if state.get("classification") else None,
                "requires_tools": state.get("classification", {}).requires_tools if state.get("classification") else False,
            },
            "workflow_metrics": metrics,
            "phases_completed": [phase.value for phase in state.get("completed_phases", [])],
            "current_phase": state.get("current_phase", {}).value if state.get("current_phase") else None,
            "tool_calls": [
                {
                    "name": tool.tool_name,
                    "success": tool.success,
                    "execution_time": tool.execution_time
                }
                for tool in state.get("tool_calls", [])
            ],
            "message_count": len(state.get("messages", [])),
            "retry_count": state.get("retry_count", 0),
            "error_count": len(state.get("errors", [])),
            "tables_used": state.get("selected_tables", []),
            "schema_context_length": len(state.get("schema_context", "")),
            **state.get("response_metadata", {})
        }
    }


def validate_messages_state(state: MessagesStateTXT2SQL) -> List[str]:
    """
    Validate MessagesState consistency
    
    Returns list of validation issues
    """
    issues = []
    
    # Check required fields
    if not state.get("user_query"):
        issues.append("Missing user_query")
    
    if not state.get("session_id"):
        issues.append("Missing session_id")
    
    if not state.get("messages"):
        issues.append("Missing messages (MessagesState primary field)")
    
    # Check message consistency
    messages = state.get("messages", [])
    if messages and not isinstance(messages[0], HumanMessage):
        issues.append("First message should be HumanMessage")
    
    # Check phase progression
    current_phase = state.get("current_phase")
    completed_phases = state.get("completed_phases", [])
    
    if current_phase in completed_phases:
        issues.append("Current phase is marked as completed")
    
    # Check tool call consistency
    tool_calls = state.get("tool_calls", [])
    tool_messages = [msg for msg in messages if isinstance(msg, ToolMessage)]
    
    if len(tool_calls) != len(tool_messages):
        issues.append("Tool calls and tool messages count mismatch")
    
    # Check retry logic
    if state.get("retry_count", 0) > state.get("max_retries", 3):
        issues.append("Retry count exceeds maximum retries")
    
    return issues


# Export factory function
create_txt2sql_messages_state = create_initial_messages_state
