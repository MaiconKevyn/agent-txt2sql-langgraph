"""
LangGraph Nodes V3 - Official SQL Agent Tutorial Patterns

Following the exact patterns from LangGraph SQL Agent tutorial:
- Tool-based nodes with proper tool calling
- MessagesState integration
- Official LangGraph best practices
- Proper error handling and routing
"""

import time
from typing import Dict, Any, List, Literal

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import BaseTool

from langchain_core.prompts import ChatPromptTemplate

from .state_v3 import (
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
from ..application.config.table_templates import build_table_specific_prompt, build_multi_table_prompt
from typing import List


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
    
    print(f"🔍 CLASSIFICATION NODE: Starting query classification")
    print(f"   📝 User Query: '{state['user_query']}'")
    
    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        
        # Create optimized classification prompt for faster response
        classification_prompt = f"""Classify this query:
        "{user_query}"
        
        Categories:
        DATABASE: data queries (count, list, show, filter)
        CONVERSATIONAL: explanations (what, how, meaning)
        SCHEMA: structure (tables, columns)
        
        Response: DATABASE/CONVERSATIONAL/SCHEMA"""
        
        # Format messages for LLM
        messages = format_for_llm_input(state, classification_prompt)
        
        # Get bound LLM (though classification doesn't need tools)
        llm = llm_manager.get_bound_llm()
        
        # Invoke LLM for classification
        response = llm.invoke(messages)
        classification_result = response.content.strip().upper()
        
        # Parse classification result
        route_mapping = {
            "DATABASE": QueryRoute.DATABASE,
            "CONVERSATIONAL": QueryRoute.CONVERSATIONAL,
            "SCHEMA": QueryRoute.SCHEMA
        }
        
        query_route = route_mapping.get(classification_result, QueryRoute.DATABASE)
        
        # Calculate confidence based on keyword matching
        confidence_score = 0.8  # Default confidence
        database_keywords = ["quantos", "count", "listar", "mostrar", "total", "média", "average"]
        conversational_keywords = ["significa", "what", "como", "why", "explain", "definition"]
        schema_keywords = ["tabelas", "tables", "colunas", "columns", "schema", "estrutura"]
        
        query_lower = user_query.lower()
        
        if any(keyword in query_lower for keyword in database_keywords):
            confidence_score = 0.9 if query_route == QueryRoute.DATABASE else 0.6
        elif any(keyword in query_lower for keyword in conversational_keywords):
            confidence_score = 0.9 if query_route == QueryRoute.CONVERSATIONAL else 0.6
        elif any(keyword in query_lower for keyword in schema_keywords):
            confidence_score = 0.9 if query_route == QueryRoute.SCHEMA else 0.6
        
        # Create classification object
        classification = QueryClassification(
            route=query_route,
            confidence_score=confidence_score,
            reasoning=f"Classified as {query_route.value} based on content analysis",
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
        
        print(f"   Classification Result: {query_route.value}")
        print(f"   Confidence: {confidence_score:.1f}")
        print(f"   Time: {execution_time:.2f}s")
        print(f"   Route: {'SQL Pipeline' if query_route == QueryRoute.DATABASE else 'Direct Response'}")
        
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
    
    print(f"🗂️ TABLE DISCOVERY NODE: Discovering available tables")
    
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
        selected_tables = _select_relevant_tables(
            user_query=state["user_query"],
            tool_result=tool_result,
            available_tables=tables,
            llm_manager=llm_manager
        )
        
        state["selected_tables"] = selected_tables
        
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
        
        print(f"   Found {len(tables)} tables: {', '.join(tables)}")
        print(f"   Selected tables: {', '.join(state['selected_tables'])}")
        print(f"   Time: {execution_time:.2f}s")
        
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
    
    print(f"SCHEMA NODE: Retrieving database schema")
    
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
        
        print(f"   Schema retrieved for tables: {tables_input}")
        print(f"   Schema context size: {len(enhanced_schema)} characters")
        print(f"   Enhanced with SUS value mappings: {('POSTGRESQL COLUMN NAMES REQUIRE' in enhanced_schema)}")
        print(f"   Time: {execution_time:.2f}s")
        
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
    
    print(f"SQL GENERATION NODE: Starting SQL generation")
    print(f"User Query: '{state['user_query']}'")
    
    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])
        
        print(f"Selected Tables: {selected_tables}")
        
        # Build table-specific prompt using our new template system
        if len(selected_tables) > 1:
            table_rules = build_multi_table_prompt(selected_tables)
            print(f"Multi-table rules applied for: {', '.join(selected_tables)}")
        else:
            table_rules = build_table_specific_prompt(selected_tables)
            print(f"Table-specific rules applied for: {', '.join(selected_tables)}")
        
        # Create ChatPromptTemplate with dynamic table rules
        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a PostgreSQL expert assistant for Brazilian healthcare (SIH-RS) data analysis.

        📋 CORE POSTGRESQL INSTRUCTIONS:
        1. Generate syntactically correct PostgreSQL queries
        2. Use proper table and column names with double quotes: "COLUMN_NAME"
        3. Handle Portuguese language questions appropriately
        4. Return only the SQL query, no explanation
        5. Use appropriate WHERE clauses for filtering
        6. Include LIMIT clauses when appropriate (default LIMIT 100)
        7. Use proper JOINs when querying multiple tables
        8. Use PostgreSQL-specific functions when needed (EXTRACT, ILIKE, etc.)
        
        🔍 DATABASE SCHEMA:
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
        
        print(f"   Using ChatPromptTemplate with {len(formatted_messages)} messages")
        print(f"   Table rules length: {len(table_rules)} chars")
        
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
            
            print(f"   ✅ SQL Generated: {sql_query}")
            
        else:
            # Handle empty SQL generation
            error_message = "Failed to generate SQL query - empty response"
            state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
            print(f"   ❌ SQL Generation Failed: Empty response")
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        print(f"   🕒 SQL Generation Time: {execution_time:.2f}s")
        
        return state
        
    except Exception as e:
        # Handle SQL generation errors
        error_message = f"SQL generation failed: {str(e)}"
        state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        print(f"   ❌ SQL Generation Error: {str(e)}")
        print(f"   🕒 Error Time: {execution_time:.2f}s")
        
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
        print(f"Response formatting failed: {e}")
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
    
    # Enhanced PostgreSQL SIH-RS mappings
    sus_mappings = """

    CRITICAL VALUE MAPPINGS FOR SIH-RS POSTGRESQL DATA - PADRÃO SUS:
    ===================================================================
    
    SEXO (Gender) - CÓDIGOS PADRÃO SUS:
    - "SEXO" = 1  →  MASCULINO/HOMEM (MALE - FOR QUESTIONS ABOUT MEN)
    - "SEXO" = 3  →  FEMININO/MULHER (FEMALE - FOR QUESTIONS ABOUT WOMEN)
    NEVER USE "SEXO" = 2 (does not exist in SUS system!)
    
    MUNIC_RES - MUNICÍPIO DE RESIDÊNCIA:
    - Contains 6-digit IBGE municipal codes (e.g., 430490 = Porto Alegre)
    - JOIN with municipios table for readable city names
    
    POSTGRESQL COLUMN NAMES REQUIRE DOUBLE QUOTES:
    - Always use "COLUMN_NAME" (with quotes) for column references
    - Example: "SEXO", "IDADE", "MUNIC_RES", "DIAG_PRINC", "CID", "CD_DESCRICAO"
    
    QUERY EXAMPLES WITH CORRECT POSTGRESQL SYNTAX:
    ===============================================
    
    ✅ TOTAL INTERNAÇÕES:
    SELECT COUNT(*) FROM internacoes;
    
    ✅ INTERNAÇÕES POR SEXO:
    -- Homens internados
    SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 1;
    
    -- Mulheres internadas  
    SELECT COUNT(*) FROM internacoes WHERE "SEXO" = 3;
    
    ✅ CIDADES COM MAIS INTERNAÇÕES (WITH JOIN):
    SELECT m."nome" as cidade, COUNT(*) as total_internacoes 
    FROM internacoes i 
    JOIN municipios m ON i."MUNIC_RES" = m."codigo" 
    GROUP BY m."nome" 
    ORDER BY total_internacoes DESC 
    LIMIT 5;
    
    ✅ DIAGNÓSTICOS MAIS COMUNS:
    SELECT c."CD_DESCRICAO", COUNT(*) as total_casos 
    FROM internacoes i 
    JOIN cid10 c ON i."DIAG_PRINC" = c."CID" 
    GROUP BY c."CD_DESCRICAO" 
    ORDER BY total_casos DESC 
    LIMIT 10;
    
    ✅ BUSCAR DESCRIÇÃO CID:
    SELECT "CD_DESCRICAO" FROM cid10 WHERE "CID" = 'F190';
    
    ✅ DIAGNÓSTICOS POR IDADE:
    SELECT "IDADE", COUNT(*) as total_casos
    FROM internacoes 
    WHERE "IDADE" IS NOT NULL 
    GROUP BY "IDADE" 
    ORDER BY "IDADE";
    
    IMPORTANT NOTES FOR POSTGRESQL SIH-RS:
    - All gender codes follow SUS official standards: 1=Masculino, 3=Feminino
    - "MUNIC_RES" contains IBGE 6-digit codes - JOIN with municipios table for names
    - Tables: internacoes (main data), cid10 (diagnoses), municipios (cities)
    - For city queries: JOIN internacoes with municipios on "MUNIC_RES" = "codigo"
    
    MANDATORY POSTGRESQL RULES:
    - "SEXO" = 1 for questions about HOMENS/MASCULINO/MEN/MALES
    - "SEXO" = 3 for questions about MULHERES/FEMININO/WOMEN/FEMALES  
    - For city names: JOIN with municipios table using "MUNIC_RES" = municipios."codigo"
    - For diagnosis descriptions: JOIN with cid10 table using "DIAG_PRINC" = cid10."CID"
    - For CID lookups: SELECT "CD_DESCRICAO" FROM cid10 WHERE "CID" = 'code'
    - ALWAYS use double quotes around ALL column names: "COLUMN_NAME"
    - CID10 columns: "CID" and "CD_DESCRICAO" (always with quotes)
    """
    
    return base_schema + sus_mappings


def _select_relevant_tables(
    user_query: str, 
    tool_result: str, 
    available_tables: List[str], 
    llm_manager: HybridLLMManager
) -> List[str]:
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
        print(f"INTELLIGENT TABLE SELECTION: Analyzing query for relevant tables")
        
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

🏥 CORE QUERIES - Primary Table Selection:
• internacoes: ALWAYS use for patient counts, general hospitalization queries
• mortes: Use ONLY when explicitly asking about deaths/mortality ("mortes", "óbitos", "falecimentos")  
• uti_detalhes: Use ONLY for ICU/intensive care queries ("UTI", "terapia intensiva", "cuidados intensivos")
• obstetricos: Use ONLY for maternal/obstetric care ("obstétricos", "gestantes", "pré-natal")

🔍 LOOKUP TABLES - Always join when names/descriptions needed:
• cid10: Join when need disease/diagnosis NAMES (not for counting patients - count from internacoes)
• procedimentos: Join when need procedure NAMES (not for counting - count from internacoes)  
• hospital: Join when need hospital/facility information
• municipios: Join when need city/municipality NAMES or geographic data

📊 SPECIALIZED ANALYSIS:
• dado_ibge: Use for socioeconomic indicators, population data, demographic analysis
• condicoes_especificas: Use for specific medical conditions (VDRL, STD screening)
• instrucao: Use for patient education level analysis

❌ AVOID THESE TABLES:
• diagnosticos_secundarios: Empty table - no data available
• infehosp: Empty table - no data available  
• cbor, vincprev: Only for very specific administrative queries

🎯 SELECTION LOGIC:
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
        
        print(f"   🤖 LLM Raw Response: '{selected_tables_str}'")
        
        # Simplified parsing with validation
        selected_tables = _parse_llm_table_selection(selected_tables_str, available_tables)
        
        # Validate selection
        selected_tables = _validate_table_selection(user_query, selected_tables, available_tables)
        
        # Final fallback: if still no valid tables, use intelligent default
        if not selected_tables:
            print(f"   ⚠️ No valid tables selected, using intelligent fallback")
            selected_tables = _get_intelligent_fallback(user_query, available_tables)
        
        print(f"   Query: '{user_query}'")
        print(f"   Available: {available_tables}")
        print(f"   Selected: {selected_tables}")
        print(f"   Intelligence: {'Single table' if len(selected_tables) == 1 else 'Multi-table'}")
        
        return selected_tables
        
    except Exception as e:
        print(f"Table selection error: {e}")
        # Fallback: retornar todas as tabelas
        return available_tables


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
    
    # Method 1: Try JSON format (if present)
    try:
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group(1))
            if 'tables' in data:
                tables = data['tables']
                selected_tables = [t for t in tables if t in available_tables]
                if selected_tables:
                    print(f"   ✅ JSON parsing successful: {selected_tables}")
                    return selected_tables
    except (json.JSONDecodeError, KeyError):
        pass
    
    # Method 2: Look for "TABLES:" section (structured response)
    tables_match = re.search(r'TABLES:\s*(.+?)(?:\n|$)', response, re.IGNORECASE)
    if tables_match:
        tables_line = tables_match.group(1).strip()
        candidate_tables = [t.strip() for t in tables_line.split(',')]
        selected_tables = [t for t in candidate_tables if t in available_tables]
        if selected_tables:
            print(f"   ✅ Structured parsing successful: {selected_tables}")
            return selected_tables
    
    # Method 3: Direct comma-separated parsing (final line preference)
    lines = response.strip().split('\n')
    for line in reversed(lines):  # Check from bottom up
        line = line.strip()
        if line and not line.startswith(('Based on', 'Therefore', 'For this', 'The', 'This')):
            # Try comma-separated
            if ',' in line:
                candidate_tables = [t.strip() for t in line.split(',')]
            else:
                candidate_tables = [line.strip()]
            
            # Clean and validate candidates
            for candidate in candidate_tables:
                clean_candidate = re.sub(r'[^a-zA-Z_]', '', candidate.strip())
                if clean_candidate in available_tables and clean_candidate not in selected_tables:
                    selected_tables.append(clean_candidate)
            
            if selected_tables:
                print(f"   ✅ Direct parsing successful: {selected_tables}")
                return selected_tables
    
    # Method 4: Search for table names anywhere in response
    for table_name in available_tables:
        if re.search(r'\b' + re.escape(table_name) + r'\b', response, re.IGNORECASE):
            if table_name not in selected_tables:
                selected_tables.append(table_name)
    
    if selected_tables:
        print(f"   ✅ Pattern matching successful: {selected_tables}")
    else:
        print(f"   ❌ No tables found in response")
    
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
    
    # Rule 1: Death queries MUST include mortes table
    if any(keyword in query_lower for keyword in ['morte', 'óbito', 'falecimento', 'mortalidade']):
        if 'mortes' not in validated_tables and 'mortes' in available_tables:
            validated_tables.append('mortes')
            print(f"   🔧 Added 'mortes' for death query")
    
    # Rule 2: Procedure frequency queries need internacoes, not procedimentos
    if any(phrase in query_lower for phrase in ['procedimentos mais comuns', 'procedimentos mais realizados', 'frequência de procedimento']):
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            print(f"   🔧 Added 'internacoes' for procedure frequency analysis")
        if 'procedimentos' in validated_tables:
            validated_tables.remove('procedimentos')
            print(f"   🔧 Removed 'procedimentos' - using internacoes for frequency")
    
    # Rule 3: Financial queries about internacoes need internacoes table
    if any(keyword in query_lower for keyword in ['valor', 'custo', 'gasto', 'financeiro']) and 'óbito' in query_lower:
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            print(f"   🔧 Added 'internacoes' for financial data")
    
    # Rule 4: Multi-table analysis validation
    if 'mortes' in validated_tables and any(keyword in query_lower for keyword in ['taxa', 'percentual', 'proporção']):
        if 'internacoes' not in validated_tables and 'internacoes' in available_tables:
            validated_tables.append('internacoes')
            print(f"   🔧 Added 'internacoes' for mortality rate calculation")
    
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
                    print(f"   🎯 Simplified to single table '{priority_table}' for simple counting")
                    break
    
    if validated_tables != selected_tables:
        print(f"   📊 Validation changes: {selected_tables} → {validated_tables}")
    
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