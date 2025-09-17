import time
from typing import Dict, Any, List, Literal

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
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
from .llm_manager import HybridLLMManager
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
from typing import List

# Initialize logger
logger = get_nodes_logger()


# Global LLM manager instance (singleton pattern)
_llm_manager: HybridLLMManager = None

def get_llm_manager() -> HybridLLMManager:
    """Get singleton LLM manager instance"""
    global _llm_manager
    if _llm_manager is None:
        config = ApplicationConfig()
        _llm_manager = HybridLLMManager(config)
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
        if detect_sql_snippets(user_query):
            query_route = QueryRoute.DATABASE
            confidence_score = 0.95
            reasoning = "Explicit SQL detected in input."
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

            llm = llm_manager.get_bound_llm()
            response = llm.invoke(messages)
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
        
        # Enhance schema context with value mappings for SUS data
        base_schema = str(tool_result)
        enhanced_schema = _enhance_sus_schema_context(base_schema)
        state["schema_context"] = enhanced_schema
        
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
        state["schema_context"] = "Tables: internacoes (patient healthcare data), cid10 (diagnoses), municipios (cities)"
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SCHEMA_ANALYSIS, execution_time)
        
        return state


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
        
        logger.info("Tables selected for SQL generation", extra={"tables": selected_tables})
        
        # Build table-specific prompt using our new template system
        if len(selected_tables) > 1:
            table_rules = build_multi_table_prompt(selected_tables)
            logger.debug("Multi-table rules applied", extra={"tables": selected_tables})
        else:
            table_rules = build_table_specific_prompt(selected_tables)
            logger.debug("Table-specific rules applied", extra={"tables": selected_tables})
        
        # Create ChatPromptTemplate with dynamic table rules
        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.

         CORE POSTGRESQL INSTRUCTIONS:
        1. Generate syntactically correct PostgreSQL queries
        2. Use proper table and column names with double quotes: "COLUMN_NAME"
        3. Handle Portuguese language questions appropriately
        4. Return only the SQL query, no explanation
        5. Use appropriate WHERE clauses for filtering
        6. Include LIMIT clauses when appropriate (default LIMIT 100)
        7. Use proper JOINs when querying multiple tables
        8. Use PostgreSQL-specific functions when needed (EXTRACT, ILIKE, etc.)
        
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
        
        # Use unbound LLM for direct SQL generation (bound LLM expects tool calls)
        llm = llm_manager._llm
        response = llm.invoke(formatted_messages)
        
        # Extract SQL query from response
        sql_query = response.content.strip() if hasattr(response, 'content') else str(response)
        
        # Clean SQL query
        sql_query = llm_manager._clean_sql_query(sql_query)
        
        if sql_query:
            state["generated_sql"] = sql_query
            
            # Add AI message with generated SQL
            ai_response = f"Generated SQL query: {sql_query}"
            state = add_ai_message(state, ai_response)
            
            logger.info("SQL generated successfully", extra={"sql": sql_query[:200]})
            
        else:
            # Handle empty SQL generation
            error_message = "Failed to generate SQL query - empty response"
            state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
            logger.warning("SQL generation failed: empty response")
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        logger.info("SQL generation completed", extra={"execution_time": execution_time})
        
        return state
        
    except Exception as e:
        # Handle SQL generation errors
        error_message = f"SQL generation failed: {str(e)}"
        state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        
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
        
        if checker_tool:
            try:
                # Execute query checker tool
                checker_result = checker_tool.invoke(generated_sql)
                validation_message = str(checker_result)
                
                # Simple validation - if no error message, consider it valid
                if "error" in validation_message.lower() or "invalid" in validation_message.lower():
                    validation_passed = False
                    
            except Exception as checker_error:
                validation_passed = False
                validation_message = f"Query checker failed: {str(checker_error)}"
        else:
            # Fallback validation using HybridLLMManager
            validation_result = llm_manager.validate_sql_query(generated_sql)
            validation_passed = validation_result["is_valid"]
            validation_message = validation_result.get("error", "Validation completed")
        
        # Update state based on validation
        if validation_passed:
            state["validated_sql"] = generated_sql
            ai_response = f"SQL query validated successfully: {generated_sql}"
        else:
            state = add_error(state, validation_message, "sql_validation_error", ExecutionPhase.SQL_VALIDATION)
            ai_response = f"SQL validation failed: {validation_message}"
        
        # Add AI message
        state = add_ai_message(state, ai_response)
        
        # Create tool call result if checker was used
        if checker_tool:
            tool_call_result = ToolCallResult(
                tool_name="sql_db_query_checker",
                tool_input={"query": generated_sql},
                tool_output=validation_message,
                success=validation_passed,
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
            execution_time = time.time() - start_time
            state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
            return state

        # Get SQL tools
        tools = llm_manager.get_sql_tools()
        query_tool = next((tool for tool in tools if tool.name == "sql_db_query"), None)
        
        if not query_tool:
            raise ValueError("sql_db_query tool not found")
        
        # Execute SQL query using tool
        tool_result = query_tool.invoke(validated_sql)
        
        # Parse results (SQLDatabaseToolkit returns string format)
        results = []
        row_count = 0
        
        if isinstance(tool_result, str) and tool_result.strip():
            # Parse string results
            lines = tool_result.strip().split('\n')
            for line in lines:
                if line.strip():
                    results.append({"result": line.strip()})
                    row_count += 1
        
        # Create execution result
        sql_execution_result = SQLExecutionResult(
            success=True,
            sql_query=validated_sql,
            results=results,
            row_count=row_count,
            execution_time=time.time() - start_time,
            validation_passed=True
        )
        
        state["sql_execution_result"] = sql_execution_result
        
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
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
        
        return state
        
    except Exception as e:
        # Handle execution errors
        error_message = f"SQL execution failed: {str(e)}"
        state = add_error(state, error_message, "sql_execution_error", ExecutionPhase.SQL_EXECUTION)
        
        # Create failed execution result
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
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_EXECUTION, execution_time)
        
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
    Enhance schema context with Brazilian SUS data value mappings
    
    Adds important value mappings that are not obvious from the schema alone
    """
    
    # Check if this is PostgreSQL SIH-RS data (contains internacoes table)
    if "internacoes" not in base_schema.lower():
        return base_schema
    
    # Enhanced PostgreSQL SIH-RS mappings based on direct DB inspection
    sus_mappings = """

    CRITICAL VALUE MAPPINGS & JOIN LOGIC FOR SIH-RS POSTGRESQL DATA:
    ===================================================================
    
    ### COLUNAS E VALORES ESSENCIAIS:
    - **SEXO (na tabela internacoes):**
      - `i."SEXO" = 1` → MASCULINO/HOMEM
      - `i."SEXO" = 3` → FEMININO/MULHER
    - **IDADE (na tabela internacoes):**
      - `i."IDADE"` → Usar para filtros de idade (ex: `i."IDADE" < 30`).
    - **MUNICÍPIO (para nomes de cidades):**
      - A tabela `municipios` contém `nome` e `codigo_6d`.
    
    ### LÓGICA DE JUNÇÃO (JOIN) OBRIGATÓRIA:
    - As tabelas `mortes` e `internacoes` **NÃO** contêm nomes de cidades, apenas códigos.
    - Para obter o **NOME DA CIDADE/MUNICÍPIO**, a junção **SEMPRE** deve seguir este caminho:
      1. Junte `mortes` com `internacoes` usando `N_AIH` (se a pergunta envolver mortes).
         - `FROM mortes mo JOIN internacoes i ON mo."N_AIH" = i."N_AIH"`
      2. Junte o resultado com `municipios` usando o código do município da tabela `internacoes`.
         - `JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d`

    ### EXEMPLOS DE CONSULTAS CORRETAS:
    =====================================
    
     **Top 10 cidades com mais mortes de idosos (> 60 anos):**
    ```sql
    SELECT mu.nome, COUNT(mo."N_AIH") AS total_mortes
    FROM mortes mo
    JOIN internacoes i ON mo."N_AIH" = i."N_AIH"
    JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
    WHERE i."IDADE" > 60 AND mo."CID_MORTE" IS NOT NULL
    GROUP BY mu.nome
    ORDER BY total_mortes DESC
    LIMIT 10;
    ```

     **Cidade com mais mortes de homens:**
    ```sql
    SELECT mu.nome, COUNT(mo."N_AIH") AS total_mortes
    FROM mortes mo
    JOIN internacoes i ON mo."N_AIH" = i."N_AIH"
    JOIN municipios mu ON i."MUNIC_RES" = mu.codigo_6d
    WHERE i."SEXO" = 1
    GROUP BY mu.nome
    ORDER BY total_mortes DESC
    LIMIT 1;
    ```

     **Contar mortes de pessoas com menos de 30 anos:**
    ```sql
    SELECT COUNT(mo."N_AIH")
    FROM mortes mo
    JOIN internacoes i ON mo."N_AIH" = i."N_AIH"
    WHERE i."IDADE" < 30;
    ```

    ### REGRAS OBRIGATÓRIAS PARA O LLM:
    1.  Para qualquer pergunta sobre **cidades/municípios**, a junção com a tabela `municipios` é obrigatória para obter o nome.
    2.  A condição de JOIN para municípios é **SEMPRE** `i."MUNIC_RES" = mu.codigo_6d`.
    3.  Se a pergunta envolve **mortes**, a condição de JOIN entre `mortes` e `internacoes` é **SEMPRE** `mo."N_AIH" = i."N_AIH"`.
    4.  Para filtros de **idade** ou **sexo**, use as colunas da tabela `internacoes` (`i."IDADE"`, `i."SEXO"`).
    5.  **SEMPRE** use aspas duplas para nomes de colunas que são case-sensitive (ex: `"N_AIH"`, `"IDADE"`).
    """
    
    return base_schema + sus_mappings


def _select_relevant_tables(
    user_query: str, 
    tool_result: str, 
    available_tables: List[str], 
    llm_manager: HybridLLMManager
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

CRITICAL SELECTION RULES FOR SIH-RS DATABASE:
====================================================

 CORE QUERIES - Primary Table Selection:
• internacoes: ALWAYS use for patient counts, general hospitalization queries
• mortes: Use ONLY when explicitly asking about deaths/mortality ("mortes", "óbitos", "falecimentos")  
• uti_detalhes: Use ONLY for ICU/intensive care queries ("UTI", "terapia intensiva", "cuidados intensivos")
• obstetricos: Use ONLY for maternal/obstetric care ("obstétricos", "gestantes", "pré-natal")

 LOOKUP TABLES - Always join when names/descriptions needed:
• cid10: Join when need disease/diagnosis NAMES (not for counting patients - count from internacoes)
• procedimentos: Join when need procedure NAMES (not for counting - count from internacoes)  
• hospital: Join when need hospital/facility information
• municipios: Join when need city/municipality NAMES or geographic data

 SPECIALIZED ANALYSIS:
• dado_ibge: Use for socioeconomic indicators, population data, demographic analysis
• condicoes_especificas: Use for specific medical conditions (VDRL, STD screening)
• instrucao: Use for patient education level analysis

 AVOID THESE TABLES:
• diagnosticos_secundarios: Empty table - no data available
• infehosp: Empty table - no data available  
• cbor, vincprev: Only for very specific administrative queries

 SELECTION LOGIC:
1. Start with internacoes for most patient/hospitalization queries
2. Add mortes ONLY if mortality is explicitly mentioned
3. Add lookup tables (cid10, procedimentos, hospital, municipios) when descriptions are needed
4. Add specialized tables only for their specific domains

USER QUERY: "{user_query}"

IMPORTANT: Respond with ONLY the table names separated by commas. No explanation or reasoning.

TABLES:"""

        # Usar LLM unbound para seleção
        llm = llm_manager._llm
        
        from langchain_core.messages import HumanMessage
        response = llm.invoke([HumanMessage(content=selection_prompt)])
        
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
    
    # Rule 1: Death queries MUST include mortes table
    if any(keyword in query_lower for keyword in ['morte', 'óbito', 'falecimento', 'mortalidade']):
        death_keywords = [k for k in ['morte', 'óbito', 'falecimento', 'mortalidade'] if k in query_lower]
        logger.info(f"Death query detected with keywords: {death_keywords}")
        if 'mortes' not in validated_tables and 'mortes' in available_tables:
            validated_tables.append('mortes')
            logger.info("Added 'mortes' table for death query")
    
    # Rule 2: Procedure frequency queries need internacoes, not procedimentos
    if any(phrase in query_lower for phrase in ['procedimentos mais comuns', 'procedimentos mais realizados', 'frequência de procedimento']):
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            logger.debug("Added 'internacoes' for procedure frequency analysis")
        if 'procedimentos' in validated_tables:
            validated_tables.remove('procedimentos')
            logger.debug("Removed 'procedimentos' - using internacoes for frequency")
    
    # Rule 3: Financial queries about internacoes need internacoes table
    if any(keyword in query_lower for keyword in ['valor', 'custo', 'gasto', 'financeiro']) and 'óbito' in query_lower:
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            logger.debug("Added 'internacoes' for financial data")
    
    # Rule 4: Multi-table analysis validation
    if 'mortes' in validated_tables and any(keyword in query_lower for keyword in ['taxa', 'percentual', 'proporção']):
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            logger.debug("Added 'internacoes' for mortality rate calculation")
    
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
            priority_tables = ['mortes', 'uti_detalhes', 'obstetricos', 'condicoes_especificas', 
                             'procedimentos', 'cid10', 'hospital', 'cbor', 'vincprev', 'instrucao']
            
            for priority_table in priority_tables:
                if priority_table in validated_tables:
                    validated_tables = [priority_table]
                    logger.debug("Simplified to single table for counting", extra={"table": priority_table})
                    break
    
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
    
    # Death-related queries
    if any(keyword in query_lower for keyword in ['morte', 'óbito', 'falecimento', 'mortalidade']):
        return ['mortes'] if 'mortes' in available_tables else ['internacoes']
    
    # UTI queries
    if any(keyword in query_lower for keyword in ['uti', 'terapia intensiva', 'cuidados intensivos']):
        return ['uti_detalhes'] if 'uti_detalhes' in available_tables else ['internacoes']
    
    # Obstetric queries
    if any(keyword in query_lower for keyword in ['obstétric', 'gestante', 'pré-natal', 'parto']):
        return ['obstetricos'] if 'obstetricos' in available_tables else ['internacoes']
    
    # CID queries
    if any(keyword in query_lower for keyword in ['cid', 'código', 'doença', 'diagnóstico']):
        return ['cid10'] if 'cid10' in available_tables else ['internacoes']
    
    # Default to internacoes for most healthcare queries
    return ['internacoes'] if 'internacoes' in available_tables else available_tables[:1]


# Export all nodes
__all__ = [
    "query_classification_node",
    "list_tables_node", 
    "get_schema_node",
    "generate_sql_node",
    "validate_sql_node",
    "execute_sql_node",
    "generate_response_node"
]
