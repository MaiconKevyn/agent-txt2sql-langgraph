import time
from datetime import datetime
from typing import Dict, Any, List, Literal, Tuple
import re
import difflib

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool

from langchain_core.prompts import ChatPromptTemplate

from .state import (
    MessagesStateTXT2SQL,
    QueryRoute,
    ExecutionPhase,
    QueryClassification,
    ToolCallResult,
    SQLExecutionResult,
    add_ai_message,
    add_tool_message,
    update_phase,
    add_error,
    add_tool_call_result,
    should_retry,
    format_for_llm_input
)
from .llm_manager import OpenAILLMManager
from ..application.config.simple_config import ApplicationConfig
from ..utils.sql_safety import is_select_only
from ..application.config.table_templates import build_table_specific_prompt, build_multi_table_prompt
from ..utils.logging_config import get_nodes_logger, TXT2SQLLogger
from ..utils.classification import (
    detect_sql_snippets,
    heuristic_route,
    try_extract_json_block,
    combine_scores,
)

# Initialize logger
logger = get_nodes_logger()


def _parse_schema_columns(schema_text: str) -> Dict[str, List[str]]:
    """Parse schema_context into {table: [columns]} using a lightweight parser."""
    if not schema_text:
        return {}
    tables: Dict[str, List[str]] = {}
    create_re = re.compile(r"CREATE\s+TABLE\s+([a-zA-Z_][\w]*)\s*\((.*?)\);", re.S | re.I)
    col_re = re.compile(r'^\s*(?:"(?P<qcol>[^"]+)"|(?P<col>[A-Za-z_][A-Za-z0-9_]*))\s+', re.M)
    for m in create_re.finditer(schema_text):
        table = (m.group(1) or "").strip()
        body = m.group(2) or ""
        cols: List[str] = []
        for cm in col_re.finditer(body):
            cname = cm.group('qcol') or cm.group('col')
            if cname and cname.upper() != 'CONSTRAINT':
                cols.append(cname)
        if cols:
            tables[table.lower()] = cols
    return tables


def _extract_alias_map(sql: str) -> Dict[str, str]:
    """Map aliases to base table names using FROM/JOIN clauses."""
    alias_map: Dict[str, str] = {}
    text = sql or ""
    for m in re.finditer(r"\bfrom\s+([a-zA-Z_][\w]*)\s+(?:as\s+)?([a-zA-Z_][\w]*)", text, flags=re.I):
        alias_map[m.group(2)] = m.group(1)
    for m in re.finditer(r"\bjoin\s+([a-zA-Z_][\w]*)\s+(?:as\s+)?([a-zA-Z_][\w]*)", text, flags=re.I):
        alias_map[m.group(2)] = m.group(1)
    return alias_map


def _extract_alias_columns(sql: str) -> List[tuple]:
    """Extract occurrences like alias.col or alias."COL"."""
    pairs: List[tuple] = []
    for m in re.finditer(r'\b([A-Za-z_][\w]*)\s*\.\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))', sql or ""):
        alias = m.group(1)
        col = m.group(2) or m.group(3)
        pairs.append((alias, col))
    return pairs


def _best_column_suggestions(missing_col: str, candidates: List[str], k: int = 3) -> List[str]:
    """Suggest k similar columns using difflib ratio and substring bonus."""
    if not candidates:
        return []
    target = (missing_col or "").lower()
    def score(c: str) -> float:
        c0 = (c or "").lower()
        r = difflib.SequenceMatcher(None, target, c0).ratio()
        if target in c0 or c0 in target:
            r += 0.1
        return r
    ranked = sorted(candidates, key=score, reverse=True)
    out, seen = [], set()
    for c in ranked:
        if c not in seen:
            out.append(c)
            seen.add(c)
        if len(out) >= k:
            break
    return out


def _check_columns_against_schema(schema_text: str, sql: str) -> Dict[str, Any]:
    """Check alias.column references against schema_context content."""
    schema_map = _parse_schema_columns(schema_text)
    alias_map = _extract_alias_map(sql)
    alias_cols = _extract_alias_columns(sql)
    missing = []
    suggestions: Dict[str, List[str]] = {}
    for alias, col in alias_cols:
        base = alias_map.get(alias)
        if not base:
            key = f"alias:{alias}"
            if key not in suggestions:
                missing.append((alias, col, None))
                suggestions[key] = []
            continue
        cols = schema_map.get((base or "").lower(), [])
        # compare case-insensitive
        if col not in cols and col.upper() not in [c.upper() for c in cols]:
            missing.append((alias, col, base))
            suggestions[f"{alias}.{col}"] = _best_column_suggestions(col, cols)
    return {"ok": len(missing) == 0, "issues": missing, "suggestions": suggestions, "alias_map": alias_map, "schema_map": schema_map}

def _should_refresh_schema(error_message: str) -> bool:
    """Detect whether the error suggests missing columns/tables."""
    if not error_message:
        return False

    lower_error = error_message.lower()

    # Direct undefined column identifiers
    if "undefined column" in lower_error or "psycopg2.errors.undefinedcolumn" in lower_error:
        return True

    # Missing relation/table markers
    missing_markers = ["does not exist", "não existe"]
    schema_terms = ["column", "coluna", "relation", "tabela", "table"]

    if any(marker in lower_error for marker in missing_markers):
        if any(term in lower_error for term in schema_terms):
            return True

    return False


def _refresh_schema_context(
    state: MessagesStateTXT2SQL,
    error_message: str,
    llm_manager: OpenAILLMManager
) -> bool:
    """Re-run table discovery and schema retrieval with error context."""
    try:
        logger.info("Refreshing schema context after execution error")

        refresh_start = time.time()

        tools = llm_manager.get_sql_tools()
        list_tables_tool = next((tool for tool in tools if tool.name == "sql_db_list_tables"), None)
        schema_tool = next((tool for tool in tools if tool.name == "sql_db_schema"), None)

        if not list_tables_tool or not schema_tool:
            raise ValueError("Required SQL tools not available for schema refresh")

        # Discover tables again
        list_tables_start = time.time()
        table_output = list_tables_tool.invoke("")
        list_tables_duration = time.time() - list_tables_start
        table_names: List[str] = []
        if isinstance(table_output, str):
            table_pattern = r'^(\w+):'
            for line in table_output.split('\n'):
                match = re.match(table_pattern, line.strip())
                if match:
                    table_names.append(match.group(1))

        if not table_names:
            db = llm_manager.get_database()
            table_names = db.get_usable_table_names()

        state["available_tables"] = table_names

        # Add error context to help table selection
        contextual_query = f"{state.get('user_query', '')}\nContexto do erro detectado: {error_message}"
        selected_tables, raw_selected_tables = _select_relevant_tables(
            user_query=contextual_query,
            tool_result=table_output,
            available_tables=table_names,
            llm_manager=llm_manager
        )

        # Fallback to initial tables if LLM returns nothing
        if not selected_tables:
            selected_tables = table_names[:3]

        state["selected_tables"] = selected_tables

        # Track refresh metadata
        metadata = state.get("response_metadata", {}) or {}
        metadata.setdefault("repair_schema_refreshes", []).append({
            "error": error_message,
            "selected_tables": selected_tables,
            "timestamp": datetime.now().isoformat()
        })
        metadata["raw_selected_tables_after_error"] = raw_selected_tables
        state["response_metadata"] = metadata

        # Record tool call
        list_tables_call = ToolCallResult(
            tool_name="sql_db_list_tables",
            tool_input={},
            tool_output=table_output,
            success=True,
            execution_time=list_tables_duration
        )
        state = add_tool_call_result(state, list_tables_call)

        # Fetch fresh schema for the new table set
        tables_input = ", ".join(selected_tables)
        schema_start = time.time()
        schema_output = schema_tool.invoke(tables_input)
        schema_duration = time.time() - schema_start
        state["schema_context"] = str(schema_output)

        schema_call = ToolCallResult(
            tool_name="sql_db_schema",
            tool_input={"tables": tables_input},
            tool_output=schema_output,
            success=True,
            execution_time=schema_duration
        )
        state = add_tool_call_result(state, schema_call)

        logger.info(
            "Schema context refreshed for repair",
            extra={
                "selected_tables": selected_tables,
                "raw_selected_tables": raw_selected_tables,
                "available_tables": table_names,
                "schema_preview": str(schema_output)[:200]
            }
        )
        return True

    except Exception as refresh_error:
        logger.warning("Failed to refresh schema context", extra={
            "error": str(refresh_error)
        })
        return False


# Global LLM manager instance (singleton pattern)
_llm_manager: OpenAILLMManager = None

def set_global_llm_manager(manager: OpenAILLMManager):
    """
    Set global LLM manager instance (called by orchestrator)

    This allows the orchestrator to inject its configured LLM manager
    into the nodes, ensuring consistency across the workflow.

    Args:
        manager: OpenAILLMManager instance to use globally
    """
    global _llm_manager
    _llm_manager = manager
    logger.info("Global LLM manager updated", extra={
        "provider": manager.config.llm_provider,
        "model": manager.config.llm_model
    })

def get_llm_manager() -> OpenAILLMManager:
    """
    Get singleton LLM manager instance

    Returns the global LLM manager that was set by the orchestrator.
    If not set, creates a default instance (fallback behavior).

    Returns:
        OpenAILLMManager instance
    """
    global _llm_manager
    if _llm_manager is None:
        # Fallback: create default instance
        # This happens if nodes are used without orchestrator
        logger.warning(
            "LLM Manager not initialized by orchestrator, using default config",
            extra={"fallback": True}
        )
        config = ApplicationConfig()
        _llm_manager = OpenAILLMManager(config)
    return _llm_manager


def query_classification_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Query Classification Node - Official LangGraph Pattern
    
    Classifies user queries to determine routing:
    - DATABASE: Requires SQL generation and execution
    - CONVERSATIONAL: Direct LLM response
    - SCHEMA: Schema introspection queries
    
    Following LangGraph SQL Agent tutorial classification approach
    """
    start_time = time.time()
    
    logger.info("Classification node started")
    
    try:
        # Extract user query from different possible state formats
        user_query = None
        
        # Try getting from user_query field (custom format)
        if "user_query" in state:
            user_query = state["user_query"]
        # Try getting from messages (LangGraph Studio format)
        elif "messages" in state and state["messages"]:
            # Get the last human message
            for msg in reversed(state["messages"]):
                if hasattr(msg, 'type') and msg.type == 'human':
                    user_query = msg.content
                    break
                elif isinstance(msg, dict) and msg.get('type') == 'human':
                    user_query = msg.get('content', '')
                    break
                elif isinstance(msg, dict) and msg.get('role') == 'human':
                    user_query = msg.get('content', '')
                    break
        
        if not user_query:
            # Debug: log state keys to understand format
            logger.debug("State parsing failed", extra={"state_keys": list(state.keys())})
            if "messages" in state:
                logger.debug("Messages found in state", extra={"messages": str(state['messages'])})
            raise ValueError("No user query found in state")
        
        logger.info("User query extracted", extra={"query": user_query[:100]})
        
        # Store user_query in state for other nodes
        if "user_query" not in state:
            state["user_query"] = user_query
        
        llm_manager = get_llm_manager()
        
        # Heuristic pre-pass
        heur_route_str, heur_scores = heuristic_route(user_query)

        # Strong early exit: explicit SQL pasted by user
        HEURISTIC_SKIP_THRESHOLD = 2
        if detect_sql_snippets(user_query):
            query_route = QueryRoute.DATABASE
            confidence_score = 0.95
            reasoning = "Explicit SQL detected in input."
        elif (
            heur_scores.get("DATABASE", 0) >= HEURISTIC_SKIP_THRESHOLD
            and heur_route_str == "DATABASE"
            and heur_scores.get("CONVERSATIONAL", 0) == 0
            and heur_scores.get("SCHEMA", 0) == 0
        ):
            # Fast-path: high-confidence DATABASE query — skip LLM call
            query_route = QueryRoute.DATABASE
            confidence_score = 0.9
            reasoning = (
                f"Heuristic fast-path: route=DATABASE, score={heur_scores.get('DATABASE', 0)}, skipping LLM"
            )
            logger.info(
                f"Heuristic fast-path: route=DATABASE, score={heur_scores.get('DATABASE', 0)}, skipping LLM"
            )
        else:
            # LLM JSON classification with few-shots
            system_prompt = (
                "Você é um classificador de consultas. Decida a ROTA em {DATABASE, CONVERSATIONAL, SCHEMA}.\n"
                "Responda APENAS em JSON com campos: {\\\"route\\\":<string>,\\\"confidence\\\":<float>,\\\"reasons\\\":<string>}\n"
                "DATABASE: perguntas de dados (contagem, ranking, listar, filtros, por cidade/ano/sexo...)\n"
                "CONVERSATIONAL: explicações/definições (\\\"o que é\\\", \\\"significa\\\", \\\"como funciona\\\", diferenças)\n"
                "SCHEMA: estrutura do banco (tabelas, colunas, schema, dicionário de dados).\n"
                "Exemplos:\n"
                "Q: Quantos óbitos ocorreram em 2023?\n"
                "A: {\\\"route\\\":\\\"DATABASE\\\",\\\"confidence\\\":0.9,\\\"reasons\\\":\\\"contagem temporal\\\"}\n"
                "Q: O que significa o CID J189?\n"
                "A: {\\\"route\\\":\\\"CONVERSATIONAL\\\",\\\"confidence\\\":0.9,\\\"reasons\\\":\\\"pedido de definição\\\"}\n"
                "Q: Quais colunas existem na tabela internacoes?\n"
                "A: {\\\"route\\\":\\\"SCHEMA\\\",\\\"confidence\\\":0.95,\\\"reasons\\\":\\\"estrutura da tabela\\\"}"
            )

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_query)
            ]

            # Use chat invocation (OpenAI)
            response = llm_manager.invoke_chat(messages)
            content = getattr(response, "content", str(response))
            data = try_extract_json_block(content)

            llm_route = None
            llm_conf = None
            llm_reasons = ""
            if isinstance(data, dict):
                r = str(data.get("route", "")).upper().strip()
                if r in ["DATABASE", "CONVERSATIONAL", "SCHEMA"]:
                    llm_route = r
                try:
                    llm_conf = float(data.get("confidence", None))
                except Exception:
                    llm_conf = None
                llm_reasons = str(data.get("reasons", "")).strip()

            threshold = 0.75
            if llm_route and llm_conf is not None and llm_conf >= threshold:
                final_route_str = llm_route
                confidence_score = float(llm_conf)
                reasoning = f"LLM(JSON) high confidence. Heuristic={heur_scores}"
            else:
                final_route_str = combine_scores(llm_route, llm_conf, heur_scores, w_llm=0.7)
                confidence_score = float(llm_conf) if llm_conf is not None else (
                    1.0 if heur_scores.get(final_route_str, 0) > 0 else 0.6
                )
                reasoning = (
                    f"Hybrid decision. llm_route={llm_route} conf={llm_conf} heur={heur_scores}"
                    + (f"; llm_reasons={llm_reasons}" if llm_reasons else "")
                )

            query_route = {
                "DATABASE": QueryRoute.DATABASE,
                "CONVERSATIONAL": QueryRoute.CONVERSATIONAL,
                "SCHEMA": QueryRoute.SCHEMA,
            }[final_route_str]

        classification = QueryClassification(
            route=query_route,
            confidence_score=confidence_score,
            reasoning=reasoning,
            requires_tools=query_route in [QueryRoute.DATABASE, QueryRoute.SCHEMA],
            estimated_complexity=0.5 if query_route == QueryRoute.CONVERSATIONAL else 0.8,
            suggested_approach=f"Use {query_route.value} processing pipeline"
        )
        
        # Update state
        state["query_route"] = query_route
        state["classification"] = classification
        state["requires_sql"] = query_route == QueryRoute.DATABASE
        
        # Add AI message with classification
        ai_response = f"Query classified as {query_route.value} (confidence: {confidence_score:.1f})"
        state = add_ai_message(state, ai_response)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.QUERY_CLASSIFICATION, execution_time)
        
        logger.info("Query classified successfully", extra={
            "result": query_route.value,
            "confidence": confidence_score,
            "execution_time": execution_time,
            "route_type": "SQL Pipeline" if query_route == QueryRoute.DATABASE else "Direct Response"
        })
        
        return state
        
    except Exception as e:
        # Handle classification errors
        error_message = f"Query classification failed: {str(e)}"
        state = add_error(state, error_message, "classification_error", ExecutionPhase.QUERY_CLASSIFICATION)
        
        # Fallback to DATABASE route
        state["query_route"] = QueryRoute.DATABASE
        state["requires_sql"] = True
        state["classification"] = QueryClassification(
            route=QueryRoute.DATABASE,
            confidence_score=0.5,
            reasoning="Fallback classification due to error",
            requires_tools=True,
            estimated_complexity=0.8,
            suggested_approach="Use database processing pipeline"
        )
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.QUERY_CLASSIFICATION, execution_time)
        
        return state


def list_tables_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    List Tables Node - Using SQLDatabaseToolkit
    
    Uses the sql_db_list_tables tool from SQLDatabaseToolkit
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()
    
    logger.info("Table discovery node started")
    
    try:
        llm_manager = get_llm_manager()
        
        # Get SQL tools
        tools = llm_manager.get_sql_tools()
        list_tables_tool = next((tool for tool in tools if tool.name == "sql_db_list_tables"), None)
        
        if not list_tables_tool:
            raise ValueError("sql_db_list_tables tool not found")
        
        # Execute list tables tool
        tool_result = list_tables_tool.invoke("")
        
        # Parse tables result from Enhanced Tool
        if isinstance(tool_result, str):
            # Enhanced tool returns table descriptions, extract actual table names
            # Look for table names at start of lines (format: "table_name: description")
            import re
            table_pattern = r'^(\w+):'
            table_names = []
            for line in tool_result.split('\n'):
                line = line.strip()
                match = re.match(table_pattern, line)
                if match:
                    table_names.append(match.group(1))
            
            # If no pattern matches, fallback to basic parsing
            if not table_names:
                # Try to extract from the database directly
                llm_manager = get_llm_manager()
                db = llm_manager.get_database()
                table_names = db.get_usable_table_names()
            
            tables = table_names
        else:
            tables = []
        
        # Update state with discovered tables
        state["available_tables"] = tables
        
        # INTELLIGENT TABLE SELECTION using LLM + Enhanced Tool context
        selected_tables, raw_selected_tables = _select_relevant_tables(
            user_query=state["user_query"],
            tool_result=tool_result,
            available_tables=tables,
            llm_manager=llm_manager
        )
        
        state["selected_tables"] = selected_tables
        # Enrich response metadata for tracing (LangSmith)
        try:
            meta = state.get("response_metadata", {}) or {}
            meta.update({
                "raw_selected_tables": raw_selected_tables,
                "validated_selected_tables": selected_tables,
            })
            state["response_metadata"] = meta
        except Exception:
            pass
        
        # Create tool call result
        tool_call_result = ToolCallResult(
            tool_name="sql_db_list_tables",
            tool_input={},
            tool_output=tool_result,
            success=True,
            execution_time=time.time() - start_time
        )
        
        # Add tool call to state
        state = add_tool_call_result(state, tool_call_result)
        
        # Add AI message
        ai_response = f"Found {len(tables)} tables: {', '.join(tables[:3])}{'...' if len(tables) > 3 else ''}"
        state = add_ai_message(state, ai_response)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.TABLE_DISCOVERY, execution_time)
        
        logger.info("Tables discovered", extra={
            "total_tables": len(tables),
            "table_list": ', '.join(tables),
            "selected_tables": ', '.join(state['selected_tables']),
            "raw_selected_tables": ', '.join(raw_selected_tables),
            "execution_time": execution_time
        })
        
        return state
        
    except Exception as e:
        # Handle table listing errors
        error_message = f"Table listing failed: {str(e)}"
        state = add_error(state, error_message, "table_discovery_error", ExecutionPhase.TABLE_DISCOVERY)
        
        # Fallback to common SUS tables
        state["available_tables"] = ["sus_data", "cid_detalhado"]
        state["selected_tables"] = ["sus_data"]
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.TABLE_DISCOVERY, execution_time)
        
        return state


def get_schema_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Get Schema Node - Using SQLDatabaseToolkit
    
    Uses the sql_db_schema tool from SQLDatabaseToolkit
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()
    
    logger.info("Schema node started")
    
    try:
        llm_manager = get_llm_manager()
        
        # Get SQL tools
        tools = llm_manager.get_sql_tools()
        schema_tool = next((tool for tool in tools if tool.name == "sql_db_schema"), None)
        
        if not schema_tool:
            raise ValueError("sql_db_schema tool not found")
        
        # Get selected tables
        selected_tables = state.get("selected_tables", [])
        if not selected_tables:
            selected_tables = state.get("available_tables", ["sus_data"])[:3]
        
        # Execute schema tool with selected tables
        tables_input = ", ".join(selected_tables)
        tool_result = schema_tool.invoke(tables_input)
        
        state["schema_context"] = str(tool_result)
        
        # Create tool call result
        tool_call_result = ToolCallResult(
            tool_name="sql_db_schema",
            tool_input={"tables": tables_input},
            tool_output=tool_result,
            success=True,
            execution_time=time.time() - start_time
        )
        
        # Add tool call to state
        state = add_tool_call_result(state, tool_call_result)
        
        # Add AI message
        schema_summary = f"Retrieved schema for {len(selected_tables)} tables"
        state = add_ai_message(state, schema_summary)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SCHEMA_ANALYSIS, execution_time)
        
        logger.info("Schema retrieved", extra={
            "tables": tables_input,
            "context_size": len(enhanced_schema),
            "sus_enhanced": 'POSTGRESQL COLUMN NAMES REQUIRE' in enhanced_schema,
            "execution_time": execution_time
        })
        
        return state
        
    except Exception as e:
        # Handle schema retrieval errors
        error_message = f"Schema retrieval failed: {str(e)}"
        state = add_error(state, error_message, "schema_error", ExecutionPhase.SCHEMA_ANALYSIS)
        
        # Fallback schema context
        state["schema_context"] = "Tables: internacoes (patient healthcare data), cid (diagnoses), municipios (cities), atendimentos (procedures junction)"

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SCHEMA_ANALYSIS, execution_time)

        return state


def _build_pregeneration_hints(selected_tables: List[str], user_query: str) -> str:
    """
    Generate targeted warnings based on selected tables.
    Injected into generate_sql_node BEFORE calling the LLM.
    Prevents predictable failure patterns (generate→fail→repair cycles).
    """
    hints = []

    if "socioeconomico" in selected_tables:
        hints.append(
            "🚨 SOCIOECONOMICO ALERT: long-format table. "
            "MANDATORY: add WHERE metrica = '<metric>' in EVERY query. "
            "Options: 'populacao_total', 'idhm', 'mortalidade_infantil_1ano', "
            "'bolsa_familia_total', 'esgotamento_sanitario_domicilio', 'taxa_envelhecimento'. "
            "Choosing wrong metric OR omitting it produces WRONG aggregations."
        )

    if "tempo" in selected_tables:
        hints.append(
            "🚨 TEMPO ALERT: NEVER join tempo table on computed expression. "
            "Use EXTRACT(YEAR/MONTH FROM DT_INTER) directly — NO JOIN. "
            "✅ WHERE EXTRACT(YEAR FROM \"DT_INTER\") = 2020  "
            "❌ JOIN tempo t ON EXTRACT(YEAR FROM i.\"DT_INTER\") = t.ano → CARTESIAN PRODUCT"
        )

    if "atendimentos" in selected_tables:
        hints.append(
            "🚨 ATENDIMENTOS ALERT: junction table pattern MANDATORY. "
            "internacoes i → JOIN atendimentos a ON i.\"N_AIH\" = a.\"N_AIH\" "
            "→ JOIN procedimentos p ON a.\"PROC_REA\" = p.\"PROC_REA\". "
            "NEVER reference a.\"NOME_PROC\" — that column is in procedimentos, not atendimentos."
        )

    if "especialidade" in selected_tables:
        hints.append(
            "🚨 ESPECIALIDADE ALERT: join on ESPEC code. "
            "JOIN especialidade e ON i.\"ESPEC\" = e.\"ESPEC\" → SELECT e.\"DESCRICAO\". "
            "For UTI: use WHERE \"VAL_UTI\" > 0 (NOT ESPEC BETWEEN 74 AND 83)."
        )

    if not hints:
        return ""

    return (
        "\n\n⚠️ TABLE-SPECIFIC WARNINGS FOR THIS QUERY:\n"
        + "\n".join(f"  {h}" for h in hints)
        + "\n"
    )


def generate_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Generate SQL Node - Using ChatPromptTemplate with Table-Specific Rules
    
    Generates SQL queries using ChatPromptTemplate with dynamic table-specific rules
    Following official LangGraph SQL agent patterns with enhanced prompt templates
    """
    start_time = time.time()
    
    logger.info("SQL generation node started", extra={
        "user_query": state['user_query'][:100]
    })
    
    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])

        # Enrich schema with value mappings (single source of truth)
        schema_context = _enhance_sus_schema_context(schema_context)

        logger.info("Tables selected for SQL generation", extra={"tables": selected_tables})
        
        # Build table-specific prompt using our new template system
        if len(selected_tables) > 1:
            table_rules = build_multi_table_prompt(selected_tables)
            logger.debug("Multi-table rules applied", extra={"tables": selected_tables})
        else:
            table_rules = build_table_specific_prompt(selected_tables)
            logger.debug("Table-specific rules applied", extra={"tables": selected_tables})

        # Inject preventive hints for known failure patterns
        pregeneration_hints = _build_pregeneration_hints(selected_tables, user_query)
        if pregeneration_hints:
            table_rules = pregeneration_hints + "\n" + table_rules
            logger.info(
                "Pre-generation hints injected",
                extra={"tables": selected_tables, "hints_length": len(pregeneration_hints)}
            )

        # Create ChatPromptTemplate with dynamic table rules
        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.

        ══════════════════════════════════════════════════════════
        CRITICAL RULES — READ THESE FIRST, THEY OVERRIDE ALL ELSE
        ══════════════════════════════════════════════════════════

        RULE A — UTI/ICU: WHERE "VAL_UTI" > 0 to count or filter UTI.
        For AVG/SUM on UTI values: also require WHERE "VAL_UTI" > 0 (excludes non-ICU zeros).
        "obstétricas"/"obstétrico" = ESPEC = 2 (NEVER ESPEC BETWEEN 74 AND 83).
        ✅ WHERE "ESPEC" = 2 AND "VAL_UTI" > 0

        RULE B — DEATH CAUSE vs DIAGNOSIS:
        "causa da morte"/"morreram de"/"óbitos por DOENÇA X" → JOIN cid ON i."CID_MORTE"=c."CID" WHERE i."MORTE"=true AND i."CID_MORTE" IS NOT NULL
        "diagnóstico principal"/"internado por DOENÇA X" → JOIN cid ON i."DIAG_PRINC"=c."CID"
        "resultaram em óbito" WITHOUT a specific disease → WHERE "MORTE"=true only (NO CID JOIN)
        ✅ "Quantas internações de UTI resultaram em óbito?" → WHERE "VAL_UTI" > 0 AND "MORTE" = true

        RULE C — LIMIT: add LIMIT only when question asks for top-N (e.g. "top 5"). NEVER add default LIMIT.

        RULE D — ONLY REQUESTED FILTERS: add only filters the question explicitly mentions.
        No age filter unless age asked. No year filter unless year asked. No gender unless gender asked.
        No "MORTE"=false unless question specifically asks for discharged/surviving patients.

        RULE E — CID COLUMN:
        • Include c."CID" WHEN: question says "com código", "código CID", or asks "quais CIDs" / "principais CIDs" / "CIDs de entrada" (CID is the subject of the question).
        • Default: SELECT only c."CD_DESCRICAO", GROUP BY c."CD_DESCRICAO".
        ✅ "principais causas de morte" → SELECT c."CD_DESCRICAO", COUNT(*) GROUP BY c."CD_DESCRICAO"
        ✅ "com código" / "código CID" → SELECT c."CID", c."CD_DESCRICAO", COUNT(*) GROUP BY c."CID", c."CD_DESCRICAO"
        ✅ "principais CIDs de entrada" / "quais os CIDs" → SELECT c."CID", c."CD_DESCRICAO", COUNT(*) GROUP BY c."CID", c."CD_DESCRICAO" ORDER BY ... LIMIT 10

        RULE F — singular "qual o X mais Y" → LIMIT 1; plural "quais os N X mais Y" → LIMIT N.

        RULE G — DATE FILTERS: use EXTRACT directly on "DT_INTER", NEVER join tempo with non-equijoin.
        ✅ WHERE EXTRACT(YEAR FROM "DT_INTER") = 2020  ← use DT_INTER for year/period filters (admission date)
        Only use "DT_SAIDA" when question explicitly asks about discharge or exit date.

        RULE H — IDADE (INTEGER) vs NASC (DATE):
        "IDADE" = pre-calculated integer age column (0-130). USE FOR ALL age filters/groupings.
        "NASC" = birth date. USE ONLY when question asks about BIRTH YEAR specifically.
        ✅ WHERE "IDADE" > 60             ✅ GROUP BY "IDADE"   ✅ CASE WHEN "IDADE" < 18
        ✅ WHERE EXTRACT(YEAR FROM "NASC") < 1950  ← "nascidos antes de 1950" → use NASC
        ❌ EXTRACT(YEAR FROM AGE("NASC")) > 60   ← NEVER! use IDADE directly
        ❌ (CURRENT_DATE - "NASC") / 365 > 60    ← NEVER! use IDADE directly

        DISEASE LOOKUP: table is "cid" (NOT "cid10"). NEVER hardcode CID codes (e.g. DIAG_PRINC = 'J18' is WRONG).
        Named disease → JOIN cid c ON i."DIAG_PRINC"=c."CID" WHERE c."CD_DESCRICAO" ILIKE '%X%'
        Category (no specific name) → WHERE "DIAG_PRINC" LIKE 'J%' (J=Respiratory, I=Cardiovascular, C=Cancer, K=Digestive)
        For cause of death by disease → JOIN cid ON i."CID_MORTE"=c."CID" (see RULE B)

        ══════════════════════════════════════════════════════════

        CORE: Use double quotes for all columns: "COLUMN_NAME". Return ONLY the SQL query.

        DATABASE SCHEMA:
        {schema_context}"""),
            
            ("system", "{table_specific_rules}"),
            
            ("human", "USER QUERY: {user_query}\n\nGenerate the SQL query:")
        ])
        
        # Format the prompt with dynamic content
        formatted_messages = sql_prompt_template.format_messages(
            schema_context=schema_context,
            table_specific_rules=table_rules,
            user_query=user_query
        )
        
        logger.debug("Template prepared", extra={
            "message_count": len(formatted_messages),
            "rules_length": len(table_rules)
        })
        
        # Use chat invocation (OpenAI)
        response = llm_manager.invoke_chat(formatted_messages)
        
        # Extract SQL query from response
        sql_query = response.content.strip() if hasattr(response, 'content') else str(response)
        
        # Clean SQL query
        sql_query = llm_manager._clean_sql_query(sql_query)
        
        if sql_query:
            state["generated_sql"] = sql_query
            # Clear previous errors on successful generation
            state["current_error"] = None
            
            # Add AI message with generated SQL
            ai_response = f"Generated SQL query: {sql_query}"
            state = add_ai_message(state, ai_response)
            
            logger.info("SQL generated successfully", extra={"sql": sql_query[:200]})
            
        else:
            # First attempt failed — retry with simplified prompt (schema + question only)
            logger.warning("SQL generation: empty response on first attempt, trying simplified prompt")
            try:
                simplified_messages = [
                    SystemMessage(content=(
                        "You are a PostgreSQL expert. Generate ONLY a valid SQL SELECT query "
                        "for the Brazilian healthcare database sihrd5. "
                        "Return ONLY the SQL, no explanation.\n\n"
                        f"DATABASE SCHEMA:\n{schema_context}"
                    )),
                    HumanMessage(content=f"USER QUERY: {user_query}\n\nGenerate the SQL query:")
                ]
                retry_response = llm_manager.invoke_chat(simplified_messages)
                retry_sql = retry_response.content.strip() if hasattr(retry_response, 'content') else str(retry_response)
                retry_sql = llm_manager._clean_sql_query(retry_sql)
                if retry_sql:
                    state["generated_sql"] = retry_sql
                    state["current_error"] = None
                    state = add_ai_message(state, f"Generated SQL (retry): {retry_sql}")
                    logger.info("SQL generated on retry", extra={"sql": retry_sql[:200]})
                else:
                    raise ValueError("Retry also produced empty SQL")
            except Exception as retry_err:
                error_message = "Failed to generate SQL query - empty response (both attempts)"
                state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
                state["retry_count"] = state.get("retry_count", 0) + 1
                state["generation_retry_count"] = state.get("generation_retry_count", 0) + 1
                logger.warning("SQL generation failed on both attempts", extra={"error": str(retry_err)})

        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        logger.info("SQL generation completed", extra={"execution_time": execution_time})
        
        return state
        
    except Exception as e:
        # Handle SQL generation errors
        error_message = f"SQL generation failed: {str(e)}"
        state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        # Persist retry counters at node level
        state["retry_count"] = state.get("retry_count", 0) + 1
        state["generation_retry_count"] = state.get("generation_retry_count", 0) + 1
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        logger.error("SQL generation failed", extra={
            "error": str(e),
            "execution_time": execution_time
        })
        
        return state


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
        
        # Get SQL tools
        tools = llm_manager.get_sql_tools()
        checker_tool = next((tool for tool in tools if tool.name == "sql_db_query_checker"), None)
        
        validation_passed = True
        validation_message = "SQL query is valid"
        checker_msg = None
        
        # Run LLM-based checker if available
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

        # Always run DB EXPLAIN validation and give precedence to it
        db_val = llm_manager.validate_sql_query(generated_sql)
        if not db_val.get("is_valid", False):
            validation_passed = False
            validation_message = db_val.get("error", "DB validation failed")
        elif validation_passed is False and db_val.get("is_valid", False):
            # LLM checker invalid but DB valid: trust DB
            validation_passed = True
            validation_message = "DB validation passed"

        # Domain semantic check: socioeconomico is long-format; must always filter by metrica
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

        # Check for tempo table cartesian explosion via non-equijoin
        if validation_passed and generated_sql:
            if re.search(r'\bjoin\s+tempo\b', generated_sql, re.I):
                # Proper equijoin: ON i."DT_INTER" = t."data" — OK (1:1 match)
                # Non-equijoin: EXTRACT(...) = t.ano, t.mes BETWEEN, etc. — CARTESIAN EXPLOSION
                has_proper_equijoin = bool(re.search(
                    r'on\s+\w+\."DT_INTER"\s*=\s*\w+\."data"',
                    generated_sql, re.I
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

        # Check for spurious VAL_UTI > 0 filter on obstetric queries without UTI mention
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

        # Check for spurious MORTE = false when question does not ask for non-death subset
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

        # Update state based on validation
        if validation_passed:
            state["validated_sql"] = generated_sql
            # Clear previous errors on successful validation
            state["current_error"] = None
            ai_response = f"SQL query validated successfully: {generated_sql}"
        else:
            state = add_error(state, validation_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
            # Persist retry counters at node level
            state["retry_count"] = state.get("retry_count", 0) + 1
            state["validation_retry_count"] = state.get("validation_retry_count", 0) + 1
            # Expose errors for debug CLI
            errs = state.get("validation_errors", []) or []
            errs.append(validation_message)
            state["validation_errors"] = errs
            ai_response = f"SQL validation failed: {validation_message}"
        
        # Add AI message
        state = add_ai_message(state, ai_response)
        
        # Create tool call result if checker was used
        if checker_tool:
            tool_call_result = ToolCallResult(
                tool_name="sql_db_query_checker",
                tool_input={"query": generated_sql},
                tool_output=checker_msg or validation_message,
                success=(checker_msg is None) or ("error" not in (checker_msg or "").lower() and "invalid" not in (checker_msg or "").lower()),
                execution_time=time.time() - start_time
            )
            state = add_tool_call_result(state, tool_call_result)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_VALIDATION, execution_time)
        
        return state
        
    except Exception as e:
        # Handle validation errors
        error_message = f"SQL validation failed: {str(e)}"
        state = add_error(state, error_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
        # Persist retry counters at node level
        state["retry_count"] = state.get("retry_count", 0) + 1
        state["validation_retry_count"] = state.get("validation_retry_count", 0) + 1
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_VALIDATION, execution_time)
        
        return state


def execute_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Execute SQL Node - Using SQLDatabaseToolkit query tool
    
    Uses the sql_db_query tool from SQLDatabaseToolkit
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()
    
    try:
        llm_manager = get_llm_manager()
        validated_sql = state.get("validated_sql") or state.get("generated_sql")
        
        if not validated_sql:
            raise ValueError("No validated SQL query to execute")
        
        # Block non-SELECT/unsafe SQL (DDL/DML or multiple statements)
        ok, reason = is_select_only(validated_sql)
        if not ok:
            error_message = f"SQL execution blocked: {reason}"
            state = add_error(state, error_message, "sql_execution_error", ExecutionPhase.SQL_EXECUTION)
            # Persist retry counters at node level
            state["retry_count"] = state.get("retry_count", 0) + 1
            state["execution_retry_count"] = state.get("execution_retry_count", 0) + 1
            execution_time = time.time() - start_time
            state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
            return state
        
        # Column existence check: skip if query was validated by DB (validated_sql set)
        if state.get("validated_sql") is None:
            schema_context = state.get("schema_context", "")
            col_check = _check_columns_against_schema(schema_context, validated_sql)
            if not col_check.get("ok", True):
                missing_items = col_check.get("issues", [])
                sugg = col_check.get("suggestions", {})
                parts = []
                for alias, col, base in missing_items:
                    key = f"{alias}.{col}"
                    cand = sugg.get(key, [])
                    base_info = f" na tabela {base}" if base else ""
                    if cand:
                        parts.append(f"Coluna ausente {key}{base_info}; candidatos: {', '.join(cand)}")
                    else:
                        parts.append(f"Coluna/alias ausente {key}{base_info}")
                msg = "; ".join(parts)
                error_message = f"SQL validation failed (schema check): {msg}"
                state = add_error(state, error_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
                # Persist retry counters at node level
                state["retry_count"] = state.get("retry_count", 0) + 1
                state["validation_retry_count"] = state.get("validation_retry_count", 0) + 1
                # Attach hints for repair
                meta = state.get("response_metadata", {}) or {}
                meta["column_check_suggestions"] = {
                    "missing": missing_items,
                    "suggestions": sugg,
                    "alias_map": col_check.get("alias_map", {}),
                    "schema_map": col_check.get("schema_map", {})
                }
                state["response_metadata"] = meta
                # Surface as AI message
                state = add_ai_message(state, f"SQL schema check falhou: {msg}")
                # Update phase and return early
                execution_time = time.time() - start_time
                state = update_phase(state, ExecutionPhase.SQL_VALIDATION, execution_time)
                return state

        # Get SQL tools
        tools = llm_manager.get_sql_tools()
        query_tool = next((tool for tool in tools if tool.name == "sql_db_query"), None)
        
        if not query_tool:
            raise ValueError("sql_db_query tool not found")

        logger.info("SQL execution started", extra={
            "sql": validated_sql
        })
        # Execute SQL query using tool
        tool_result = query_tool.invoke(validated_sql)
        
        # Parse results (SQLDatabaseToolkit returns string format)
        results = []
        row_count = 0
        execution_success = True
        error_message = None

        if isinstance(tool_result, str) and tool_result.strip():
            tool_result_str = tool_result.strip()

            # Check for common SQL error patterns in tool response
            error_indicators = [
                "does not exist",
                "não existe",
                "ERRO:",
                "ERROR:",
                "psycopg2.errors",
                "column.*not found",
                "coluna.*não existe",
                "relation.*does not exist",
                "tabela.*não existe",
                "invalid sql",
                "syntax error"
            ]

            # Check if tool result contains error message
            lower_result = tool_result_str.lower()
            if any(indicator.lower() in lower_result for indicator in error_indicators):
                execution_success = False
                error_message = tool_result_str
                logger.error("SQL tool returned error", extra={
                    "error_in_result": tool_result_str
                })
            else:
                # Parse normal results
                lines = tool_result_str.split('\n')
                for line in lines:
                    if line.strip():
                        results.append({"result": line.strip()})
                        row_count += 1

        # Create execution result
        sql_execution_result = SQLExecutionResult(
            success=execution_success,
            sql_query=validated_sql,
            results=results,
            row_count=row_count,
            execution_time=time.time() - start_time,
            validation_passed=True,
            error_message=error_message
        )
        
        state["sql_execution_result"] = sql_execution_result

        # If execution failed, propagate error and let workflow handle retry
        if not execution_success:
            # Set error in state to trigger retry routing
            state = add_error(state, error_message, "sql_execution_error", ExecutionPhase.SQL_EXECUTION)
            # Persist retry counters at node level
            state["retry_count"] = state.get("retry_count", 0) + 1
            state["execution_retry_count"] = state.get("execution_retry_count", 0) + 1

            # Create failed tool call result
            tool_call_result = ToolCallResult(
                tool_name="sql_db_query",
                tool_input={"query": validated_sql},
                tool_output=tool_result,
                success=False,
                execution_time=time.time() - start_time
            )

            # Add tool call to state
            state = add_tool_call_result(state, tool_call_result)

            # Add AI message about the error
            ai_response = f"SQL execution failed: {error_message}"
            state = add_ai_message(state, ai_response)

            # Log the failure for debugging
            logger.error("SQL execution failed with tool error", extra={
                "sql": validated_sql[:200],
                "error": error_message
            })

            execution_time = time.time() - start_time
            state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)

            return state

        # Success path
        # Create tool call result
        tool_call_result = ToolCallResult(
            tool_name="sql_db_query",
            tool_input={"query": validated_sql},
            tool_output=tool_result,
            success=True,
            execution_time=time.time() - start_time
        )

        # Add tool call to state
        state = add_tool_call_result(state, tool_call_result)

        # Add AI message with results
        ai_response = f"Query executed successfully. Found {row_count} results."
        if row_count > 0 and results:
            # Show first few results
            sample_results = results[:3]
            ai_response += f" Sample: {sample_results}"

        state = add_ai_message(state, ai_response)

        # Successful execution clears previous error context and retry counters
        state["current_error"] = None
        state["retry_count"] = 0
        
        # Update phase
        execution_time = time.time() - start_time
        logger.info("Query executed successfully", extra={
            "sql": validated_sql[:200],
            "row_count": row_count,
            "execution_time": execution_time
        })
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)

        return state
        
    except Exception as e:
        # Handle execution errors and surface them as tool output
        error_message = f"SQL execution failed: {str(e)}"
        logger.error("SQL execution failed", extra={
            "sql": validated_sql if validated_sql else "",
            "error": str(e)
        })
        state = add_error(state, error_message, "sql_execution_error", ExecutionPhase.SQL_EXECUTION)
        # Persist retry counters at node level
        state["retry_count"] = state.get("retry_count", 0) + 1
        state["execution_retry_count"] = state.get("execution_retry_count", 0) + 1

        # Record failed execution result for downstream logic
        sql_execution_result = SQLExecutionResult(
            success=False,
            sql_query=validated_sql or "",
            results=[],
            row_count=0,
            execution_time=time.time() - start_time,
            validation_passed=False,
            error_message=error_message
        )
        state["sql_execution_result"] = sql_execution_result

        # Surface the error to the LLM as a tool response (LangGraph ToolNode pattern)
        try:
            state = add_tool_message(
                state,
                tool_call_id=f"call_{len(state['tool_calls']) + 1}",
                content=error_message,
                tool_name="sql_db_query"
            )
            logger.debug("Execution error propagated as ToolMessage", extra={
                "tool_call_id": len(state["tool_calls"]),
                "message": error_message
            })
        except Exception:
            # If tool message injection fails, continue without blocking error propagation
            pass

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)

        return state


def repair_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """Repair SQL Node - regenerate SQL after execution failure."""

    start_time = time.time()

    try:
        llm_manager = get_llm_manager()
        previous_sql = state.get("generated_sql")

        if not previous_sql:
            raise ValueError("No SQL available for repair")

        # Retrieve latest SQL execution error message from tool feedback
        error_message = state.get("current_error") or ""
        logger.info("Repair node triggered", extra={
            "previous_sql": previous_sql[:200],
            "current_error": error_message
        })
        for msg in reversed(state.get("messages", [])):
            if isinstance(msg, ToolMessage) and getattr(msg, "name", "") == "sql_db_query":
                error_message = msg.content
                break

        if not error_message:
            error_message = "Erro desconhecido ao executar a consulta."  # fallback context

        user_query = state.get("user_query", "")

        # Refresh table/schema context when missing columns/relations are detected
        if _should_refresh_schema(error_message):
            refreshed = _refresh_schema_context(state, error_message, llm_manager)
            logger.info(
                "Schema refresh attempted during repair",
                extra={
                    "refreshed": refreshed,
                    "selected_tables": state.get("selected_tables", []),
                    "available_tables": state.get("available_tables", [])
                }
            )

        selected_tables = state.get("selected_tables", [])
        schema_context = state.get("schema_context", "") or ""

        # Build whitelist and suggestions based on previous SQL and schema
        col_check = _check_columns_against_schema(schema_context, previous_sql)
        schema_map = col_check.get("schema_map", {})
        alias_map = col_check.get("alias_map", {})
        meta_for_suggestions = state.get("response_metadata", {}) or {}
        column_hints = meta_for_suggestions.get("column_check_suggestions", {})
        missing = column_hints.get("missing", [])
        sugg_map = column_hints.get("suggestions", {})

        # Limit schema context to avoid excessively large prompts
        MAX_SCHEMA_CHARS = 4000
        if len(schema_context) > MAX_SCHEMA_CHARS:
            schema_context = schema_context[:MAX_SCHEMA_CHARS] + "\n... (schema truncado)"

        # Prepare whitelist per alias (limit columns for brevity)
        whitelist_lines = []
        for alias, table in alias_map.items():
            cols = schema_map.get((table or "").lower(), [])
            if cols:
                preview = ", ".join(cols[:50]) + (" ..." if len(cols) > 50 else "")
                whitelist_lines.append(f"Alias {alias} → tabela {table}: {preview}")
        whitelist_text = "\n".join(whitelist_lines) if whitelist_lines else "(não encontrado)"

        # Suggestions for missing columns
        suggestion_lines = []
        for a, c, base in missing:
            key = f"{a}.{c}"
            cands = sugg_map.get(key, [])
            if cands:
                suggestion_lines.append(f"{key} → candidatos: {', '.join(cands)}")
        suggestions_text = "\n".join(suggestion_lines) if suggestion_lines else "(sem sugestões)"

        system_prompt = (
            "Você é um especialista em PostgreSQL responsável por corrigir consultas SQL para o banco SUS. "
            "Restrições obrigatórias: USE APENAS colunas da lista branca por alias/tabela; se uma coluna não existir, substitua por uma das sugeridas; "
            "corrija os JOINs usando chaves que existam em ambas as tabelas. "
            "CRÍTICO — ASPAS DUPLAS: em PostgreSQL TODOS os nomes de colunas DEVEM usar aspas duplas. "
            "Se o erro mencionar 'coluna X não existe', quase sempre é falta de aspas duplas — adicione-as: "
            "c.cd_descricao → c.\"CD_DESCRICAO\"; c.cid → c.\"CID\"; alias.coluna → alias.\"COLUNA\". "
            "Responda apenas com a SQL válida, sem comentários, markdown ou texto adicional."
        )

        human_prompt = (
            f"Consulta do usuário (contexto):\n{user_query}\n\n"
            f"SQL anterior gerada:\n{previous_sql}\n\n"
            f"Erro retornado pelo banco de dados/validação:\n{error_message}\n\n"
            f"Tabelas selecionadas: {', '.join(selected_tables) if selected_tables else 'N/D'}\n\n"
            f"Lista branca de colunas por alias/tabela:\n{whitelist_text}\n\n"
            f"Sugestões de substituição de colunas ausentes:\n{suggestions_text}\n\n"
            f"Schema disponível:\n{schema_context}\n\n"
            "Reescreva a consulta corrigindo o problema identificado, usando SOMENTE colunas da lista branca e as sugestões quando necessário."
        )
        
        # Use chat invocation for SQL repair (OpenAI)
        response = llm_manager.invoke_chat([
            SystemMessage(content=system_prompt),
            HumanMessage(content=human_prompt)
        ])

        repaired_sql = response.content.strip() if hasattr(response, "content") else str(response)
        repaired_sql = llm_manager._clean_sql_query(repaired_sql)

        if not repaired_sql:
            raise ValueError("LLM returned empty SQL during repair")

        # Early-exit if repeated same SQL across last two attempts
        def _norm(s: str) -> str:
            return re.sub(r"\s+", "", (s or "").lower()).rstrip(";")
        meta = state.get("response_metadata", {}) or {}
        history = meta.get("repair_attempts", [])
        prevs = []
        if history:
            prevs.append(history[-1].get("previous_sql", ""))
        prevs.append(previous_sql)
        if len(prevs) >= 2 and all(_norm(p) == _norm(repaired_sql) for p in prevs):
            diag = "Reparo produziu a mesma SQL das últimas tentativas. Use apenas colunas válidas conforme lista branca e sugestões."
            state = add_error(state, diag, "sql_repair_error", ExecutionPhase.SQL_REPAIR)
            state["retry_count"] = state.get("max_retries", 3)
            meta["repair_early_exit"] = {
                "reason": diag,
                "whitelist": whitelist_lines,
                "suggestions": suggestion_lines
            }
            state["response_metadata"] = meta
            execution_time = time.time() - start_time
            state = update_phase(state, ExecutionPhase.SQL_REPAIR, execution_time)
            return state

        # Store previous SQL attempt for traceability
        metadata = state.get("response_metadata", {}) or {}
        repair_history = metadata.get("repair_attempts", [])
        repair_history.append({
            "previous_sql": previous_sql,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat()
        })
        metadata["repair_attempts"] = repair_history
        state["response_metadata"] = metadata

        state["generated_sql"] = repaired_sql

        # Reset error context so downstream routing treats this as a fresh attempt
        state["current_error"] = None
        # state["retry_count"] = 0
        logger.info("Repaired SQL generated", extra={
            "new_sql": repaired_sql[:200]
        })

        ai_message = (
            "Gerada nova versão da consulta após erro de execução."
        )
        state = add_ai_message(state, ai_message)

        execution_time = time.time() - start_time
        # Track repair phase completion
        state = update_phase(state, ExecutionPhase.SQL_REPAIR, execution_time)

        logger.info("SQL repair completed successfully", extra={
            "sql": repaired_sql[:200],
            "selected_tables": selected_tables
        })

        return state

    except Exception as e:
        error_message = f"SQL repair failed: {str(e)}"
        state = add_error(state, error_message, "sql_repair_error", ExecutionPhase.SQL_GENERATION)

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_REPAIR, execution_time)

        logger.warning("SQL repair failed", extra={
            "error": str(e)
        })

        return state


def generate_response_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Generate Response Node - Format final response
    
    Generates natural language response based on query results or provides conversational response
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()
    
    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        query_route = state.get("query_route", QueryRoute.DATABASE)
        
        if query_route == QueryRoute.CONVERSATIONAL:
            # Generate conversational response
            result = llm_manager.generate_conversational_response(
                user_query=user_query,
                conversation_history=state["messages"]
            )
            
            if result["success"]:
                final_response = result["response"]
            else:
                final_response = f"Desculpe, não consegui processar sua pergunta: {result.get('error', 'Erro desconhecido')}"
                
        else:
            # Generate response based on SQL execution results
            sql_execution_result = state.get("sql_execution_result")
            
            if sql_execution_result and sql_execution_result.success:
                # Use LLM to format response in a user-friendly way
                final_response = _generate_formatted_response(
                    llm_manager=llm_manager,
                    user_query=user_query,
                    sql_query=sql_execution_result.sql_query,
                    results=sql_execution_result.results,
                    row_count=sql_execution_result.row_count
                )
            else:
                # Handle errors
                error_message = state.get("current_error", "Erro desconhecido")
                final_response = f"Não foi possível processar sua consulta: {error_message}"
        
        # Update state with final response
        state["final_response"] = final_response
        state["success"] = not bool(state.get("current_error"))
        state["completed"] = True
        
        # Add final AI message
        state = add_ai_message(state, final_response)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.COMPLETED, execution_time)
        
        return state
        
    except Exception as e:
        # Handle response generation errors
        error_message = f"Response generation failed: {str(e)}"
        state = add_error(state, error_message, "response_generation_error", ExecutionPhase.RESPONSE_FORMATTING)
        
        # Fallback response
        state["final_response"] = f"Erro interno: {error_message}"
        state["success"] = False
        state["completed"] = True
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.COMPLETED, execution_time)
        
        return state


def _generate_formatted_response(
    llm_manager,
    user_query: str,
    sql_query: str,
    results: List[Dict[str, Any]],
    row_count: int
) -> str:
    """
    Generate user-friendly formatted response using LLM
    
    Uses the LLM to interpret SQL results and create natural language responses
    that are more readable and informative for end users.
    
    Implements output limiting to prevent excessively long responses.
    """
    try:
        # Handle empty results
        if row_count == 0:
            return "Nenhum resultado encontrado para sua consulta."
        
        # SAFETY LIMITS - Prevent excessively long responses
        MAX_RESULTS_TO_SHOW = 10
        MAX_RESULT_STRING_LENGTH = 1000  # Limit individual result strings
        MAX_TOTAL_RESULTS_LENGTH = 5000  # Limit total results text
        
        # Prepare results for formatting with safety limits
        results_text = ""
        if row_count == 1 and len(results) == 1:
            # Single result - extract the actual value with length limit
            result_value = results[0].get("result", "")
            result_str = str(result_value)
            
            # Apply length limit to prevent huge single results
            if len(result_str) > MAX_RESULT_STRING_LENGTH:
                results_text = result_str[:MAX_RESULT_STRING_LENGTH] + f"... (resultado truncado, {len(result_str)} caracteres total)"
            else:
                results_text = result_str
        else:
            # Multiple results with comprehensive limiting
            results_to_show = min(len(results), MAX_RESULTS_TO_SHOW)
            
            for i, result in enumerate(results[:results_to_show], 1):
                result_value = result.get('result', '')
                result_str = str(result_value)
                
                # Limit individual result length
                if len(result_str) > MAX_RESULT_STRING_LENGTH:
                    result_str = result_str[:MAX_RESULT_STRING_LENGTH] + "..."
                
                line = f"{i}. {result_str}\n"
                
                # Check if adding this line would exceed total length limit
                if len(results_text) + len(line) > MAX_TOTAL_RESULTS_LENGTH:
                    results_text += f"... (saída truncada para evitar resposta excessivamente longa)\n"
                    break
                    
                results_text += line
            
            # Add count information
            if row_count > results_to_show:
                results_text += f"... (mostrando {results_to_show} de {row_count} resultados)"
        
        # Final safety check - ensure total results text isn't too long
        if len(results_text) > MAX_TOTAL_RESULTS_LENGTH:
            results_text = results_text[:MAX_TOTAL_RESULTS_LENGTH] + "... (resposta truncada por segurança)"
        
        # Create prompt for response formatting
        formatting_prompt = f"""Transforme o resultado técnico em uma resposta natural e concisa em português.

        Pergunta: "{user_query}"
        Resultado: {results_text}
        
        REGRAS IMPORTANTES:
        1. Seja CONCISO
        2. Responda APENAS o que foi perguntado
        3. Use linguagem natural em português brasileiro
        4. Formate números adequadamente (1.234 não 1234)
        5. NÃO adicione explicações extras, disclaimers ou ofertas de ajuda
        6. NÃO mencione SQL, tabelas ou detalhes técnicos
        
        EXEMPLOS:
        Pergunta: "Quantos pacientes existem?" → "Existem 24.485 pacientes cadastrados."
        Pergunta: "Qual cidade com mais mortes de homens?" → "A cidade onde morreram mais homens foi Ijuí, com 212 mortes."
        Pergunta: "Quantas mulheres?" → "Existem 15.234 pacientes do sexo feminino."
        
        Resposta concisa:"""

        # Use conversational response method for formatting
        format_result = llm_manager.generate_conversational_response(
            user_query=formatting_prompt,
            context=None,
            conversation_history=[]
        )
        
        if format_result["success"]:
            formatted_response = format_result["response"].strip()
            
            # FINAL SAFETY CHECK - Limit total response length
            MAX_FINAL_RESPONSE_LENGTH = 2000
            if len(formatted_response) > MAX_FINAL_RESPONSE_LENGTH:
                formatted_response = formatted_response[:MAX_FINAL_RESPONSE_LENGTH] + "... (resposta limitada por segurança)"
            
            # Basic validation - if response is too short or seems broken, fallback
            if len(formatted_response) < 10 or "erro" in formatted_response.lower():
                return _generate_fallback_response(user_query, results_text, row_count)
            
            return formatted_response
        else:
            # Fallback to basic formatting if LLM fails
            return _generate_fallback_response(user_query, results_text, row_count)
            
    except Exception as e:
        logger.error("Response formatting failed", extra={"error": str(e)})
        # Fallback to basic formatting
        return _generate_fallback_response(user_query, results_text if 'results_text' in locals() else str(results), row_count)


def _generate_fallback_response(user_query: str, results_text: str, row_count: int) -> str:
    """Generate basic fallback response when LLM formatting fails"""
    
    # Apply safety limits to fallback response as well
    MAX_FALLBACK_LENGTH = 1000
    
    # Limit results_text for fallback
    if len(results_text) > MAX_FALLBACK_LENGTH:
        results_text = results_text[:MAX_FALLBACK_LENGTH] + "... (resposta truncada)"
    if row_count == 0:
        return "Nenhum resultado encontrado para sua consulta."
    elif row_count == 1:
        # Try to make single results more readable
        if results_text.strip().startswith("[('") and results_text.strip().endswith("')]"):
            # Parse tuple format like "[('Ijuí', 212)]"
            try:
                import ast
                parsed = ast.literal_eval(results_text.strip())
                if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], tuple):
                    if len(parsed[0]) == 2:
                        city, count = parsed[0]
                        return f"Resultado: {city} com {count:,} registros."
                    elif len(parsed[0]) == 1:
                        value = parsed[0][0]
                        if isinstance(value, (int, float)):
                            return f"Resultado: {value:,}"
                        else:
                            return f"Resultado: {value}"
            except:
                pass
        
        # Basic single result formatting
        return f"Resultado: {results_text}"
    else:
        # Multiple results
        return f"Encontrados {row_count} resultados:\n{results_text}"


def _enhance_sus_schema_context(base_schema: str) -> str:
    """
    Enhance schema context with Brazilian SUS data value mappings.

    Adds critical value mappings and join rules not obvious from raw DDL.
    This is the single source of truth for column semantics — overrides DDL if conflicting.
    """
    sus_mappings = """

CRITICAL COLUMN VALUE MAPPINGS (sihrd5 — override DDL if conflicting):
=======================================================================
internacoes:
  "SEXO"    INTEGER: 1=Masculino, 3=Feminino (NUNCA usar 2)
  "MORTE"   BOOLEAN: true=óbito, false=alta
  "IND_VDRL" BOOLEAN: true=positivo (filtrar sem JOIN cid)
  "IDADE"   INTEGER (0-130): idade pré-calculada — USAR para todos filtros de idade
  "NASC"    DATE: data de nascimento — usar SOMENTE para "nascidos antes/após ano X"
            ❌ EXTRACT(YEAR FROM AGE("NASC")) — ERRADO, usar "IDADE" diretamente
            ❌ (CURRENT_DATE - "NASC") / 365 > 60 — ERRADO, usar "IDADE" diretamente
  "VAL_TOT" NUMERIC: custo total   | "VAL_SH": serviço hospitalar | "VAL_UTI": UTI
            "valor do serviço hospitalar" → VAL_SH  (NÃO VAL_TOT!)
  "ESPEC"   INTEGER: 1=Cirúrgico, 2=Obstétrico, 3=Clínico, 4=Crônico, 5=Psiquiatria, 7=Pediátrico
  "MUNIC_RES" FK→municipios.codigo_6d: município de RESIDÊNCIA do paciente
  "MUNIC_MOV" FK→hospital.MUNIC_MOV: município do hospital (localização do hospital)
  "DIAG_PRINC" FK→cid."CID": diagnóstico principal de entrada
  "CID_MORTE"  FK→cid."CID": causa da morte (somente quando MORTE=true)

socioeconomico (long-format — SEMPRE filtrar por metrica):
  metrica='populacao_total'             | metrica='idhm'
  metrica='mortalidade_infantil_1ano'   | metrica='bolsa_familia_total'
  metrica='esgotamento_sanitario_domicilio' | metrica='taxa_envelhecimento'
  ⚠️ SEM WHERE metrica=? → SUM soma TODAS as métricas → resultado sem sentido!

raca_cor:
  0/99=Sem info, 1=Branca, 2=Preta, 3=Parda, 4=Amarela, 5=Indígena
  Filtrar inline: WHERE "RACA_COR" = 5 (sem JOIN)
  Descrição: JOIN raca_cor r ON i."RACA_COR" = r."RACA_COR" → SELECT r."DESCRICAO"

JOIN RULES:
  municipio do paciente → JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
  municipio do hospital → JOIN hospital h ON ... JOIN municipios mu ON h."MUNIC_MOV" = mu.codigo_6d
  especialidade         → JOIN especialidade e ON i."ESPEC" = e."ESPEC" → SELECT e."DESCRICAO"
  diagnóstico           → JOIN cid c ON i."DIAG_PRINC" = c."CID" → SELECT c."CD_DESCRICAO"
  causa de morte        → JOIN cid c ON i."CID_MORTE" = c."CID" WHERE i."MORTE" = true
"""
    return base_schema + sus_mappings


def _heuristic_table_selection(
    user_query: str, available_tables: List[str]
) -> Tuple[List[str], float]:
    """
    Stage 1: keyword-based fast table selection.
    Returns (tables, confidence). If confidence >= 0.85, skips LLM call.
    """
    q = user_query.lower()

    if re.search(r'mortalidade infantil|taxa de mortalidade infantil', q):
        return (['socioeconomico'], 0.95)

    # Hospital mortality rate (NOT infant) — always from internacoes
    if re.search(r'taxa de mortalidade|mortalidade hospitalar|maior taxa de mortalidade', q) \
            and 'infantil' not in q:
        return (['internacoes', 'municipios'], 0.93)

    if re.search(r'procedimentos?\s+(mais\s+)?(comuns?|realizados?|frequentes?)', q):
        return (['internacoes', 'atendimentos', 'procedimentos'], 0.92)

    if re.search(r'idhm|bolsa\s*familia|saneamento|pop.*econom', q):
        return (['socioeconomico', 'municipios'], 0.92)

    if re.search(r'especialidade.*interna[cç][oõ]es|interna[cç][oõ]es.*especialidade', q):
        return (['internacoes', 'especialidade'], 0.90)

    if re.search(r'n[ií]vel\s+de\s+instru[cç][aã]o|escolaridade.*interna[cç][oõ]es', q):
        return (['internacoes', 'instrucao'], 0.90)

    if re.search(r'ra[cç]a.*mortes|mortes.*ra[cç]a|ra[cç]a.*interna[cç][oõ]es', q):
        return (['internacoes', 'raca_cor'], 0.90)

    if re.search(r'hospital.*munic[ií]pio|munic[ií]pio.*hospital', q):
        return (['internacoes', 'hospital', 'municipios'], 0.88)

    return ([], 0.0)


def _select_relevant_tables(
    user_query: str,
    tool_result: str,
    available_tables: List[str],
    llm_manager: OpenAILLMManager
) -> (List[str], List[str]):
    """
    Seleciona tabelas relevantes usando LLM + contexto das descrições completas
    
    Args:
        user_query: Pergunta do usuário
        tool_result: Output da Enhanced Tool com descrições
        available_tables: Lista de todas as tabelas disponíveis
        llm_manager: Manager do LLM
        
    Returns:
        Lista de tabelas selecionadas relevantes para a query
    """
    try:
        logger.info("Intelligent table selection started")

        # Stage 1: heuristic fast-path
        heur_tables, heur_confidence = _heuristic_table_selection(user_query, available_tables)
        if heur_confidence >= 0.85 and heur_tables:
            logger.info(
                f"Heuristic table selection: {heur_tables} (conf={heur_confidence})"
            )
            validated = _validate_table_selection(user_query, heur_tables, available_tables)
            return validated, heur_tables

        # Stage 2: LLM selection (only for ambiguous queries)
        logger.info("Heuristic inconclusive — falling back to LLM table selection")

        # Import table descriptions
        from ..application.config.table_descriptions import TABLE_DESCRIPTIONS

        # Build comprehensive table selection prompt using actual descriptions
        table_desc_lines = []
        for table_name in available_tables:
            if table_name in TABLE_DESCRIPTIONS:
                desc = TABLE_DESCRIPTIONS[table_name]
                title = desc.get("title", table_name)
                purpose = desc.get("purpose", "")
                use_cases = desc.get("use_cases", [])
                critical_notes = desc.get("critical_notes", [])
                
                # Create concise but informative description
                line = f"- {table_name}: {title}"
                if purpose:
                    line += f" | {purpose}"
                if use_cases:
                    line += f" | Use for: {', '.join(use_cases[:2])}"
                if critical_notes:
                    line += f" | {'; '.join(critical_notes[:2])}"
                
                table_desc_lines.append(line)
            else:
                # Fallback for tables not in descriptions
                table_desc_lines.append(f"- {table_name}: Database table")
        
        # Create streamlined selection prompt for structured response
        selection_prompt = f"""POSTGRESQL TABLE SELECTION - Brazilian SUS Healthcare System

AVAILABLE TABLES:
{chr(10).join(table_desc_lines)}

CRITICAL SELECTION RULES FOR sihrd5 DATABASE:
====================================================

 CORE QUERIES - Primary Table Selection:
• internacoes: ALWAYS use for patient counts, hospitalization queries, deaths (MORTE boolean), VDRL (IND_VDRL boolean)
• atendimentos: Use when asking about procedures performed per hospitalization (junction table)

 LOOKUP TABLES - Always join when names/descriptions needed:
• cid: Join when need disease/diagnosis NAMES (table renamed from cid10 — use cid, NOT cid10!)
• procedimentos: Join when need procedure NAMES (always via atendimentos junction table)
• hospital: Join when need hospital/facility information
• municipios: Join when need city/municipality NAMES or geographic data

 SPECIALIZED ANALYSIS — socioeconomico is the ONLY source for these metrics:
• socioeconomico: Use for "população", "mortalidade infantil", "IDHM", "bolsa família", "envelhecimento",
  "saneamento", "dados do IBGE", "dados socioeconômicos" — ALWAYS filter by metrica column.
  NEVER use internacoes for "taxa de mortalidade infantil" — that comes from socioeconomico!
  Example: WHERE metrica = 'mortalidade_infantil_1ano' OR metrica = 'populacao_total' OR metrica = 'idhm'
• instrucao: Lookup for education level descriptions (JOIN with internacoes.INSTRU)
• vincprev: Lookup for social security type descriptions (JOIN with internacoes.VINCPREV)
• especialidade: Lookup for medical specialty descriptions (JOIN with internacoes.ESPEC)
• raca_cor: Lookup for race/color descriptions (JOIN with internacoes.RACA_COR)

 DISAMBIGUATION RULES:
• "taxa de mortalidade" / "mortalidade hospitalar" / "percentual de óbitos" in hospitalization context →
  calculate from internacoes: COUNT(MORTE=true)/COUNT(*). DO NOT use socioeconomico.
• "mortalidade infantil" / "taxa de mortalidade infantil" → socioeconomico with metrica='mortalidade_infantil_1ano'
• "municípios de residência" / "onde os pacientes moram" → internacoes.MUNIC_RES → municipios
• "municípios que atendem" / "por localização do hospital" / "média por município (hospital)" →
  hospital.MUNIC_MOV → municipios (requires BOTH hospital AND municipios tables)

 TABLES THAT NO LONGER EXIST (DO NOT SELECT):
• mortes: REMOVED — use internacoes WHERE "MORTE" = true
• cid10: RENAMED to cid — use cid
• dado_ibge: REPLACED by socioeconomico
• uti_detalhes: REMOVED — use internacoes."VAL_UTI" > 0 to identify ICU admissions
• condicoes_especificas: REMOVED — use internacoes WHERE "IND_VDRL" = true
• obstetricos: REMOVED — use internacoes.INSC_PN, GESTRICO, CONTRACEP1, CONTRACEP2
• cbor, infehosp, diagnosticos_secundarios: REMOVED from sihrd5

 SELECTION LOGIC:
1. Start with internacoes for most patient/hospitalization queries
2. For deaths/mortality: use internacoes with WHERE "MORTE" = true (no separate mortes table)
3. For procedures: add atendimentos + procedimentos (junction pattern)
4. Add lookup tables (cid, hospital, municipios) when descriptions are needed
5. For socioeconomic data: use socioeconomico (not dado_ibge)

USER QUERY: "{user_query}"

IMPORTANT: Respond with ONLY the table names separated by commas. No explanation or reasoning.

TABLES:"""

        # Usar invocação de chat agnóstica de provider para seleção
        from langchain_core.messages import HumanMessage
        response = llm_manager.invoke_chat([HumanMessage(content=selection_prompt)])
        
        # Parse response using simplified approach
        selected_tables_str = response.content.strip() if hasattr(response, 'content') else str(response)
        
        logger.info(f"LLM table selection response: {selected_tables_str}")
        
        # Simplified parsing with validation
        selected_tables = _parse_llm_table_selection(selected_tables_str, available_tables)
        raw_selected_tables = list(selected_tables)
        
        logger.info(f"Tables after parsing: {selected_tables}")
        
        # Validate selection
        selected_tables = _validate_table_selection(user_query, selected_tables, available_tables)
        
        logger.info(f"Tables after validation: {selected_tables}")
        
        # Final fallback: if still no valid tables, use intelligent default
        if not selected_tables:
            logger.warning("No valid tables selected, using fallback")
            selected_tables = _get_intelligent_fallback(user_query, available_tables)
        
        logger.info("Table selection completed", extra={
            "query": user_query[:100],
            "available": available_tables,
            "selected": selected_tables,
            "raw_selected": raw_selected_tables,
            "type": "Single table" if len(selected_tables) == 1 else "Multi-table"
        })
        
        return selected_tables, raw_selected_tables
        
    except Exception as e:
        logger.error("Table selection failed", extra={"error": str(e)})
        # Fallback: retornar todas as tabelas
        return available_tables, available_tables


def _parse_llm_table_selection(response: str, available_tables: List[str]) -> List[str]:
    """
    Simplified parsing of LLM table selection response
    
    Args:
        response: Raw LLM response
        available_tables: List of valid table names
        
    Returns:
        List of valid table names extracted from response
    """
    import re
    import json
    
    selected_tables = []
    
    logger.info("Starting LLM response parsing", extra={"raw_response": response[:200]})
    
    # Method 1: Try JSON format (if present)
    try:
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(1))
            if 'tables' in data:
                tables = data['tables']
                selected_tables = [t for t in tables if t in available_tables]
                if selected_tables:
                    logger.debug("JSON parsing successful", extra={"tables": selected_tables})
                    return selected_tables
    except (json.JSONDecodeError, KeyError):
        logger.debug("JSON parsing failed or not found")
        pass
    
    # Method 2: Look for "TABLES:" section (structured response)
    tables_match = re.search(r'TABLES:\s*(.+?)(?:\n|$)', response, re.IGNORECASE)
    if tables_match:
        tables_line = tables_match.group(1).strip()
        logger.debug("Found TABLES: section", extra={"tables_line": tables_line})
        candidate_tables = [t.strip() for t in tables_line.split(',')]
        logger.debug("Candidate tables from TABLES: section", extra={"candidates": candidate_tables})
        selected_tables = [t for t in candidate_tables if t in available_tables]
        if selected_tables:
            logger.debug("Structured parsing successful", extra={"tables": selected_tables})
            return selected_tables
    
    # Method 3: Direct comma-separated parsing (first line preference)
    lines = response.strip().split('\n')
    for line in lines:  # Check from top to bottom to prioritize first line
        line = line.strip()
        # Skip empty lines and obvious notes/comments
        if not line or line.startswith(('(Note:', 'Note:', 'Based on', 'Therefore', 'For this', 'The selection', 'I selected')):
            continue
            
        # Try comma-separated parsing
        if ',' in line:
            candidate_tables = [t.strip() for t in line.split(',')]
        else:
            candidate_tables = [line.strip()]
        
        # Clean and validate candidates
        valid_candidates = []
        for candidate in candidate_tables:
            # Remove non-alphanumeric characters except underscores
            clean_candidate = re.sub(r'[^a-zA-Z0-9_]', '', candidate.strip())
            if clean_candidate in available_tables:
                valid_candidates.append(clean_candidate)
        
        if valid_candidates:
            logger.info(f"Direct parsing successful from line: '{line}' -> {valid_candidates}")
            return valid_candidates
    
    # Method 4: Search for table names anywhere in response
    for table_name in available_tables:
        if re.search(r'\b' + re.escape(table_name) + r'\b', response, re.IGNORECASE):
            if table_name not in selected_tables:
                selected_tables.append(table_name)
    
    if selected_tables:
        logger.debug("Pattern matching successful", extra={"tables": selected_tables})
    else:
        logger.warning("No tables found in LLM response")
    
    return selected_tables


def _validate_table_selection(user_query: str, selected_tables: List[str], available_tables: List[str]) -> List[str]:
    """
    Validate and enhance table selection using business rules
    
    Args:
        user_query: User's query
        selected_tables: Tables selected by LLM
        available_tables: All available tables
        
    Returns:
        Validated and potentially enhanced table list
    """
    import re
    
    query_lower = user_query.lower()
    validated_tables = selected_tables.copy()
    
    logger.info(f"Starting table validation - Query: '{user_query}' - Initial: {selected_tables}")
    
    # Rule 1: Death queries — in sihrd5 MORTE is a boolean in internacoes (no separate mortes table)
    # EXCEPTION: "mortalidade infantil" rate lives in socioeconomico — do NOT add internacoes
    if any(keyword in query_lower for keyword in ['morte', 'óbito', 'falecimento', 'mortalidade']):
        is_infant_mortality = 'infantil' in query_lower and any(k in query_lower for k in ['mortalidade', 'taxa'])
        death_keywords = [k for k in ['morte', 'óbito', 'falecimento', 'mortalidade'] if k in query_lower]
        if is_infant_mortality:
            logger.info(f"Infant mortality query detected — keeping socioeconomico, NOT adding internacoes")
        else:
            logger.info(f"Death query detected with keywords: {death_keywords}")
            if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
                validated_tables.append('internacoes')
                logger.info("Added 'internacoes' for death query (MORTE boolean column)")
    
    # Rule 2: Procedure frequency queries need internacoes + atendimentos + procedimentos
    if any(phrase in query_lower for phrase in ['procedimentos mais comuns', 'procedimentos mais realizados', 'frequência de procedimento', 'procedimento']):
        added_tables = []
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            added_tables.append('internacoes')
        if 'atendimentos' not in validated_tables and 'atendimentos' in available_tables:
            validated_tables.append('atendimentos')
            added_tables.append('atendimentos')
        if 'procedimentos' not in validated_tables and 'procedimentos' in available_tables:
            validated_tables.append('procedimentos')
            added_tables.append('procedimentos')
        if added_tables:
            logger.debug("Ensured tables for procedure frequency", extra={"added": added_tables, "tables": validated_tables})
    
    # Rule 3: Financial queries about internacoes need internacoes table
    if any(keyword in query_lower for keyword in ['valor', 'custo', 'gasto', 'financeiro']) and 'óbito' in query_lower:
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            logger.debug("Added 'internacoes' for financial data")
    
    # Rule 4: Hospital mortality rate → internacoes only (NOT socioeconomico).
    # "taxa de mortalidade" without "infantil" is always hospitalization-derived.
    # We must also REMOVE socioeconomico if the LLM mistakenly added it.
    if any(kw in query_lower for kw in ['taxa de mortalidade', 'taxa mortalidade', 'mortalidade']) and \
       any(kw in query_lower for kw in ['taxa', 'percentual', 'proporção', 'maior taxa', 'municípios com']):
        if 'infantil' not in query_lower:
            if 'socioeconomico' in validated_tables:
                validated_tables.remove('socioeconomico')
                logger.info(
                    "Removed 'socioeconomico': hospital mortality rate uses internacoes, NOT socioeconomico"
                )
            if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
                validated_tables.append('internacoes')
                logger.info("Added 'internacoes' for hospital mortality rate calculation (MORTE boolean)")
    
    # Rule 4b: Obstetric queries — only internacoes needed, ESPEC = 2 pattern (never a cid JOIN)
    # "obstétricas" maps to ESPEC = 2 in internacoes, NOT to CID codes
    if any(keyword in query_lower for keyword in ['obstétric', 'obstétrica', 'obstétricas', 'obstétrico']):
        if 'internacoes' in validated_tables:
            validated_tables = ['internacoes']  # single table only, ESPEC = 2 handles it
            logger.info("Reduced to internacoes only for obstetric query — use ESPEC = 2")
        elif 'cid' in validated_tables:
            validated_tables.remove('cid')
            logger.info("Removed 'cid' for obstetric query — use ESPEC = 2 instead of CID join")

    # Rule 4c: Infant mortality queries — only socioeconomico needed (simple single-table AVG)
    if 'infantil' in query_lower and any(k in query_lower for k in ['mortalidade', 'taxa']):
        if 'socioeconomico' in validated_tables:
            validated_tables = ['socioeconomico']
            logger.info("Simplified to socioeconomico only for infant mortality rate query")

    # Rule 5: Remove unnecessary over-selections for simple counting
    if len(validated_tables) > 1:
        simple_counting_patterns = [
            r'quantos? \w+ foram registrad[ao]s?',
            r'quantos? \w+ exist[em]?',
            r'total de \w+'
        ]
        is_simple_count = any(re.search(pattern, query_lower) for pattern in simple_counting_patterns)
        
        if is_simple_count and not any(join_keyword in query_lower for join_keyword in ['por', 'com', 'em', 'de']):
            # Keep only the most specific table for simple counting
            priority_tables = ['atendimentos', 'procedimentos', 'cid',
                             'hospital', 'socioeconomico', 'vincprev', 'instrucao', 'especialidade']
            
            for priority_table in priority_tables:
                if priority_table in validated_tables:
                    validated_tables = [priority_table]
                    logger.debug("Simplified to single table for counting", extra={"table": priority_table})
                    break

    # Rule 6: JOIN dependency - atendimentos requires internacoes and procedimentos
    if 'atendimentos' in validated_tables:
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            logger.info("Added 'internacoes' - required for atendimentos junction")
        if 'procedimentos' not in validated_tables and 'procedimentos' in available_tables:
            validated_tables.append('procedimentos')
            logger.info("Added 'procedimentos' - required for atendimentos junction")

    if validated_tables != selected_tables:
        logger.debug("Table validation completed", extra={
            "original": selected_tables,
            "validated": validated_tables
        })
    
    return validated_tables


def _get_intelligent_fallback(user_query: str, available_tables: List[str]) -> List[str]:
    """
    Intelligent fallback when no tables are selected
    
    Args:
        user_query: User's query
        available_tables: Available tables
        
    Returns:
        Intelligent default table selection
    """
    query_lower = user_query.lower()
    
    # Death-related queries — MORTE is a boolean in internacoes (no separate mortes table)
    if any(keyword in query_lower for keyword in ['morte', 'óbito', 'falecimento', 'mortalidade']):
        return ['internacoes']

    # UTI queries — use VAL_UTI > 0 in internacoes (no uti_detalhes table; never use ESPEC for UTI)
    if any(keyword in query_lower for keyword in ['uti', 'terapia intensiva', 'cuidados intensivos']):
        return ['internacoes']

    # Obstetric queries — obstetric data is in internacoes (INSC_PN, GESTRICO, etc.)
    if any(keyword in query_lower for keyword in ['obstétric', 'gestante', 'pré-natal', 'parto']):
        return ['internacoes']

    # Procedure queries
    if any(keyword in query_lower for keyword in ['procedimento', 'cirurgia', 'tratamento']):
        return ['atendimentos', 'internacoes', 'procedimentos'] if 'atendimentos' in available_tables else ['internacoes']

    # CID queries
    if any(keyword in query_lower for keyword in ['cid', 'código', 'doença', 'diagnóstico']):
        return ['cid'] if 'cid' in available_tables else ['internacoes']
    
    # Default to internacoes for most healthcare queries
    return ['internacoes'] if 'internacoes' in available_tables else available_tables[:1]


# Export all nodes
__all__ = [
    "query_classification_node",
    "list_tables_node", 
    "get_schema_node",
    "generate_sql_node",
    "repair_sql_node",
    "validate_sql_node",
    "execute_sql_node",
    "generate_response_node"
]
