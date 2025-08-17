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
        
        print(f"   ✅ Classification Result: {query_route.value}")
        print(f"   📊 Confidence: {confidence_score:.1f}")
        print(f"   🕒 Time: {execution_time:.2f}s")
        print(f"   🎯 Route: {'SQL Pipeline' if query_route == QueryRoute.DATABASE else 'Direct Response'}")
        
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
        
        # Parse tables result
        if isinstance(tool_result, str):
            tables = [table.strip() for table in tool_result.split(",") if table.strip()]
        else:
            tables = []
        
        # Update state with discovered tables
        state["available_tables"] = tables
        
        # For now, select all available tables (can be refined later)
        state["selected_tables"] = tables[:5]  # Limit to first 5 tables
        
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
        
        print(f"   ✅ Found {len(tables)} tables: {', '.join(tables)}")
        print(f"   🎯 Selected tables: {', '.join(state['selected_tables'])}")
        print(f"   🕒 Time: {execution_time:.2f}s")
        
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
    
    print(f"📋 SCHEMA NODE: Retrieving database schema")
    
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
        
        print(f"   ✅ Schema retrieved for tables: {tables_input}")
        print(f"   📊 Schema context size: {len(enhanced_schema)} characters")
        print(f"   🔧 Enhanced with SUS value mappings: {('SEXO = 1' in enhanced_schema)}")
        print(f"   🕒 Time: {execution_time:.2f}s")
        
        return state
        
    except Exception as e:
        # Handle schema retrieval errors
        error_message = f"Schema retrieval failed: {str(e)}"
        state = add_error(state, error_message, "schema_error", ExecutionPhase.SCHEMA_ANALYSIS)
        
        # Fallback schema context
        state["schema_context"] = "Tables: sus_data (patient healthcare data)"
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SCHEMA_ANALYSIS, execution_time)
        
        return state


def generate_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Generate SQL Node - Using LLM with Schema Context
    
    Generates SQL queries using the bound LLM with schema context
    Following official LangGraph SQL agent patterns
    """
    start_time = time.time()
    
    try:
        llm_manager = get_llm_manager()
        user_query = state["user_query"]
        schema_context = state.get("schema_context", "")
        
        # Create enhanced SQL generation prompt with SUS value mappings
        sql_prompt = f"""You are a SQL expert assistant for Brazilian healthcare (SUS) data. Follow SUS standards EXACTLY.

        Database Schema:
        {schema_context}
        
        User Question: {user_query}
        
        🚨 CRITICAL INSTRUCTIONS:
        1. Generate syntactically correct SQLite queries
        2. Use proper table and column names from the schema
        3. Handle Portuguese language questions appropriately
        4. Return only the SQL query, no explanation
        5. Use appropriate WHERE clauses for filtering
        6. Include LIMIT clauses when appropriate (default LIMIT 100)
        
        ⚠️  MANDATORY SUS VALUE MAPPINGS - NEVER MAKE MISTAKES:
        - For MEN/HOMENS/MASCULINO questions: ALWAYS use SEXO = 1
        - For WOMEN/MULHERES/FEMININO questions: ALWAYS use SEXO = 3
        - For DEATH/MORTE/ÓBITO questions: ALWAYS use MORTE = 1
        - For CITY/CIDADE questions: ALWAYS use CIDADE_RESIDENCIA_PACIENTE
        
        🎯 EXACT PATTERN EXAMPLES:
        - "Quantos homens morreram?" → SELECT COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1;
        - "Qual cidade com mais mortes de homens?" → SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY COUNT(*) DESC LIMIT 1;
        
        Generate the SQL query now:"""
        
        # Format messages for LLM
        messages = format_for_llm_input(state, sql_prompt)
        
        # Use HybridLLMManager's SQL generation method
        result = llm_manager.generate_sql_query(
            user_query=user_query,
            schema_context=schema_context,
            conversation_history=state["messages"]
        )
        
        if result["success"]:
            sql_query = result["sql_query"]
            state["generated_sql"] = sql_query
            
            # Add AI message with generated SQL
            ai_response = f"Generated SQL query: {sql_query}"
            state = add_ai_message(state, ai_response)
            
        else:
            # Handle SQL generation failure
            error_message = result.get("error", "Unknown SQL generation error")
            state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        
        # Update phase
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
        return state
        
    except Exception as e:
        # Handle SQL generation errors
        error_message = f"SQL generation failed: {str(e)}"
        state = add_error(state, error_message, "sql_generation_error", ExecutionPhase.SQL_GENERATION)
        
        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, execution_time)
        
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
    """
    try:
        # Handle empty results
        if row_count == 0:
            return "Nenhum resultado encontrado para sua consulta."
        
        # Prepare results for formatting
        results_text = ""
        if row_count == 1 and len(results) == 1:
            # Single result - extract the actual value
            result_value = results[0].get("result", "")
            results_text = str(result_value)
        else:
            # Multiple results
            for i, result in enumerate(results[:10], 1):  # Show up to 10 results
                results_text += f"{i}. {result.get('result', '')}\n"
            
            if row_count > 10:
                results_text += f"... (total de {row_count} resultados)"
        
        # Create prompt for response formatting
        formatting_prompt = f"""Transforme o resultado técnico em uma resposta natural e concisa em português.

        Pergunta: "{user_query}"
        Resultado: {results_text}
        
        REGRAS IMPORTANTES:
        1. Seja CONCISO - máximo 1-2 frases
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
            
            # Basic validation - if response is too short or seems broken, fallback
            if len(formatted_response) < 10 or "erro" in formatted_response.lower():
                return _generate_fallback_response(user_query, results_text, row_count)
            
            return formatted_response
        else:
            # Fallback to basic formatting if LLM fails
            return _generate_fallback_response(user_query, results_text, row_count)
            
    except Exception as e:
        print(f"⚠️  Response formatting failed: {e}")
        # Fallback to basic formatting
        return _generate_fallback_response(user_query, results_text if 'results_text' in locals() else str(results), row_count)


def _generate_fallback_response(user_query: str, results_text: str, row_count: int) -> str:
    """Generate basic fallback response when LLM formatting fails"""
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
    
    # Check if this is SUS data (contains sus_data table)
    if "sus_data" not in base_schema.lower():
        return base_schema
    
    # Enhanced SUS mappings using knowledge from original services
    sus_mappings = """

    🚨 CRITICAL VALUE MAPPINGS FOR SUS DATA - PADRÃO SUS OFICIAL:
    ============================================================
    
    SEXO (Gender) - CÓDIGOS PADRÃO SUS:
    - SEXO = 1  →  MASCULINO/HOMEM (MALE - FOR QUESTIONS ABOUT MEN)
    - SEXO = 3  →  FEMININO/MULHER (FEMALE - FOR QUESTIONS ABOUT WOMEN)
    ⚠️  NEVER USE SEXO = 2 (does not exist in SUS system!)
    
    MORTE (Death) - INDICADOR DE ÓBITO:
    - MORTE = 0  →  NÃO (ALIVE/LIVING - patient did not die)
    - MORTE = 1  →  SIM (DEAD/DECEASED - patient died)
    
    CIDADE/MUNICÍPIO - CRITICAL RULES:
    - ✅ ALWAYS USE: CIDADE_RESIDENCIA_PACIENTE (readable city names like 'Porto Alegre', 'Santa Maria')
    - ❌ NEVER USE: MUNIC_RES (contains only IBGE numeric codes like 430300, 430460 - useless for end users)
    
    🎯 QUERY EXAMPLES WITH CORRECT SUS VALUES:
    =========================================
    
    ✅ MORTES POR SEXO (CORRECT MAPPING):
    -- Homens que morreram
    SELECT COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1
    
    -- Mulheres que morreram  
    SELECT COUNT(*) FROM sus_data WHERE SEXO = 3 AND MORTE = 1
    
    ✅ CIDADES COM MAIS MORTES DE HOMENS (COMPLETE EXAMPLE):
    SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as mortes_homens 
    FROM sus_data 
    WHERE SEXO = 1 AND MORTE = 1 
    GROUP BY CIDADE_RESIDENCIA_PACIENTE 
    ORDER BY mortes_homens DESC 
    LIMIT 5
    
    ✅ PACIENTES HOMENS POR DIAGNÓSTICO:
    SELECT DIAG_PRINC, COUNT(*) as total_homens 
    FROM sus_data 
    WHERE SEXO = 1 
    GROUP BY DIAG_PRINC 
    ORDER BY total_homens DESC
    
    ✅ PACIENTES MULHERES POR DIAGNÓSTICO:
    SELECT DIAG_PRINC, COUNT(*) as total_mulheres 
    FROM sus_data 
    WHERE SEXO = 3 
    GROUP BY DIAG_PRINC 
    ORDER BY total_mulheres DESC
    
    IMPORTANT NOTES FROM SUS SYSTEM:
    - All gender codes follow SUS official standards: 1=Masculino, 3=Feminino
    - CIDADE_RESIDENCIA_PACIENTE contains human-readable city names
    - MUNIC_RES contains only numeric IBGE codes (like 430300) - NOT useful for city queries
    - MORTE = 1 indicates patient death/óbito during hospitalization
    - For city/municipality questions, ALWAYS use CIDADE_RESIDENCIA_PACIENTE column
    
    ⚠️  MANDATORY: SEXO = 1 for ANY question about HOMENS/MASCULINO/MEN/MALES
    ⚠️  MANDATORY: SEXO = 3 for ANY question about MULHERES/FEMININO/WOMEN/FEMALES
    ⚠️  MANDATORY: Use CIDADE_RESIDENCIA_PACIENTE for city names, NEVER MUNIC_RES
    """
    
    return base_schema + sus_mappings


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