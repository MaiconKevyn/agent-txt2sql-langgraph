import os
from typing import List, Dict, Any, Optional, Union
from langchain_core.language_models import BaseLLM
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import BaseTool
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_ollama import ChatOllama
from langchain_community.llms import HuggingFacePipeline

# Adicionar suporte a novos provedores LLM
try:
    from langchain_groq import ChatGroq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from ..application.config.simple_config import ApplicationConfig
from ..utils.sql_safety import is_select_only, sanitize_sql_for_execution
from ..utils.logging_config import get_llm_manager_logger

# Initialize logger
logger = get_llm_manager_logger()


class HybridLLMManager:
    """
    Hybrid LLM Manager following LangGraph SQL Agent best practices
    
    Integrates:
    - SQLDatabaseToolkit for database operations
    - Multi-provider LLM support (Ollama, HuggingFace)
    - Tool binding with llm.bind_tools()
    - Message state management
    - Official LangGraph patterns
    """
    
    def __init__(self, config: ApplicationConfig):
        self.config = config
        self._llm: Optional[BaseLLM] = None
        self._sql_database: Optional[SQLDatabase] = None
        self._sql_toolkit: Optional[SQLDatabaseToolkit] = None
        self._bound_llm = None
        
        # Performance optimization: cache for expensive operations
        self._schema_cache = {}
        self._table_list_cache = None
        self._cache_timeout = 300  # 5 minutes cache
        
        # Initialize components
        self._initialize_database()
        self._initialize_llm()
        self._initialize_sql_toolkit()
        self._bind_tools()
    
    def _initialize_database(self):
        """Initialize SQLDatabase for PostgreSQL (LangChain integration)"""
        try:
            db_path = self.config.database_path or ""
            if not (self.config.database_type == "postgresql" or db_path.startswith("postgresql")):
                raise ValueError("Database must be PostgreSQL. Defina DATABASE_URL no .env ou use --db-url.")

            # Normalize driver style if needed
            connection_string = db_path
            if connection_string.startswith("postgresql+psycopg2://"):
                connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

            # Redact credentials before logging
            redacted = connection_string
            try:
                if "://" in connection_string and "@" in connection_string:
                    scheme_sep = connection_string.split("://", 1)
                    right = scheme_sep[1]
                    if "@" in right:
                        after_at = right.split("@", 1)[1]
                        redacted = f"{scheme_sep[0]}://****@{after_at}"
            except Exception:
                redacted = "[redacted]"
            logger.info("Connecting to PostgreSQL", extra={"connection_string": redacted})
            
            # Create SQLDatabase instance following LangGraph tutorial
            self._sql_database = SQLDatabase.from_uri(connection_string)
            
            # Verify database connection
            table_names = self._sql_database.get_usable_table_names()
            if not table_names:
                raise ValueError("No usable tables found in database")
                
            logger.info("SQLDatabase initialized", extra={"table_count": len(table_names)})
            
        except Exception as e:
            logger.error("Database initialization failed", extra={"error": str(e)})
            raise
    
    def _initialize_llm(self):
        """Initialize LLM based on provider configuration"""
        try:
            provider = self.config.llm_provider.lower()
            model_name = self.config.llm_model
            
            if provider == "ollama":
                self._llm = ChatOllama(
                    model=model_name,
                    temperature=self.config.llm_temperature,
                    timeout=self.config.llm_timeout,
                    num_predict=1024,  # Reduced for faster response
                    top_k=5,  # Reduced for more focused responses
                    top_p=0.9
                )
                
            elif provider == "groq":
                # GROQ - LLM OPEN SOURCE GRATUITO COM TOOL CALLING
                if not GROQ_AVAILABLE:
                    raise ImportError("Groq not available. Install with: pip install groq langchain-groq")
                
                api_key = os.getenv("GROQ_API_KEY")
                if not api_key:
                    raise ValueError("GROQ_API_KEY environment variable not set")
                
                self._llm = ChatGroq(
                    model=model_name,
                    temperature=self.config.llm_temperature,
                    groq_api_key=api_key,
                    max_tokens=None,
                    timeout=None,
                    max_retries=2
                )
                logger.info("Groq LLM initialized", extra={"model": model_name, "description": "Free Llama3 70B with tool calling"})
                
            elif provider == "openai":
                if not OPENAI_AVAILABLE:
                    raise ImportError("OpenAI not available. Install with: pip install openai langchain-openai")
                
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    raise ValueError("OPENAI_API_KEY environment variable not set")
                
                self._llm = ChatOpenAI(
                    model=model_name,
                    temperature=self.config.llm_temperature,
                    api_key=api_key,
                    timeout=self.config.llm_timeout,
                    max_retries=2
                )
                logger.info("OpenAI LLM initialized", extra={"model": model_name})
                
            elif provider == "huggingface":
                self._llm = HuggingFacePipeline.from_model_id(
                    model_id=model_name,
                    task="text-generation",
                    device=0 if self.config.llm_device == "cuda" else -1,
                    model_kwargs={
                        "temperature": self.config.llm_temperature,
                        "load_in_8bit": self.config.llm_load_in_8bit,
                        "load_in_4bit": self.config.llm_load_in_4bit,
                        "max_new_tokens": 2048
                    }
                )
                
            else:
                supported_providers = ["ollama", "huggingface"]
                if GROQ_AVAILABLE:
                    supported_providers.append("groq")
                if OPENAI_AVAILABLE:
                    supported_providers.append("openai")
                raise ValueError(f"Unsupported LLM provider: {provider}. Supported: {supported_providers}")
            
            logger.info("LLM initialized", extra={"model": model_name, "provider": provider})
            
        except Exception as e:
            logger.error("LLM initialization failed", extra={"error": str(e)})
            raise
    
    def _initialize_sql_toolkit(self):
        """Initialize SQLDatabaseToolkit with Enhanced Tools following LangGraph patterns"""
        try:
            if not self._llm or not self._sql_database:
                raise ValueError("LLM and database must be initialized first")
            
            # Create SQLDatabaseToolkit as per LangGraph tutorial
            self._sql_toolkit = SQLDatabaseToolkit(
                db=self._sql_database,
                llm=self._llm
            )
            
            # Get standard tools from toolkit
            standard_tools = self._sql_toolkit.get_tools()
            
            # Create enhanced tools by replacing sql_db_list_tables with our custom version
            enhanced_tools = self._create_enhanced_tools(standard_tools)
            
            # Store enhanced tools for use
            self._enhanced_tools = enhanced_tools
            
            logger.info("Enhanced SQLDatabaseToolkit initialized", extra={"tool_count": len(enhanced_tools)})
            
            # Log available tools (including enhanced ones)
            for tool in enhanced_tools:
                tool_type = " Enhanced" if "Enhanced" in str(type(tool).__name__) else " Standard"
                logger.debug("Tool loaded", extra={
                    "type": tool_type,
                    "name": tool.name,
                    "description": tool.description[:80]
                })
                
        except Exception as e:
            logger.error("SQLDatabaseToolkit initialization failed", extra={"error": str(e)})
            raise
    
    def _create_enhanced_tools(self, standard_tools: List[BaseTool]) -> List[BaseTool]:
        """
        Create enhanced version of SQL tools by replacing standard ones with enhanced versions
        
        Args:
            standard_tools: Original tools from SQLDatabaseToolkit
            
        Returns:
            List of enhanced tools with custom implementations
        """
        try:
            # Import our enhanced tool
            from .tools.enhanced_list_tables_tool import EnhancedListTablesTool
            
            # Filter out the original sql_db_list_tables tool
            enhanced_tools = [
                tool for tool in standard_tools 
                if tool.name != "sql_db_list_tables"
            ]
            
            # Create and add our enhanced list tables tool
            enhanced_list_tool = EnhancedListTablesTool(db=self._sql_database)
            enhanced_tools.append(enhanced_list_tool)
            
            logger.info("Enhanced sql_db_list_tables tool integrated", extra={
                "original_tools": len(standard_tools),
                "enhanced_tools": len(enhanced_tools),
                "replacement": "sql_db_list_tables → EnhancedListTablesTool"
            })
            
            return enhanced_tools
            
        except ImportError as e:
            logger.warning("Enhanced tools not available, falling back to standard", extra={"error": str(e)})
            return standard_tools
        except Exception as e:
            logger.error("Error creating enhanced tools, falling back to standard", extra={"error": str(e)})
            return standard_tools
    
    def _bind_tools(self):
        """Bind enhanced tools to LLM following LangGraph best practices"""
        try:
            if not self._llm or not self._sql_toolkit:
                raise ValueError("LLM and toolkit must be initialized first")
            
            # Get enhanced tools (or standard as fallback)
            tools = self.get_sql_tools()
            
            # Bind tools to LLM (official LangGraph pattern)
            self._bound_llm = self._llm.bind_tools(tools)
            
            # Count enhanced vs standard tools for logging
            enhanced_count = sum(1 for tool in tools if "Enhanced" in str(type(tool).__name__))
            standard_count = len(tools) - enhanced_count
            
            logger.info("Tools bound to LLM", extra={"tool_count": len(tools)})
            if enhanced_count > 0:
                logger.debug("Tool breakdown", extra={
                    "enhanced_count": enhanced_count,
                    "standard_count": standard_count
                })
            
        except Exception as e:
            logger.error("Tool binding failed", extra={"error": str(e)})
            # Fallback: use unbound LLM
            self._bound_llm = self._llm
    
    def get_sql_tools(self) -> List[BaseTool]:
        """Get enhanced SQL database tools"""
        # Return enhanced tools if available, otherwise fall back to standard tools
        if hasattr(self, '_enhanced_tools') and self._enhanced_tools:
            return self._enhanced_tools
        elif self._sql_toolkit:
            return self._sql_toolkit.get_tools()
        else:
            return []
    
    def get_bound_llm(self) -> BaseLLM:
        """Get LLM with bound tools"""
        return self._bound_llm or self._llm
    
    def get_database(self) -> SQLDatabase:
        """Get SQLDatabase instance"""
        return self._sql_database
    
    def create_messages(
        self, 
        user_query: str, 
        system_prompt: Optional[str] = None,
        conversation_history: Optional[List[BaseMessage]] = None
    ) -> List[BaseMessage]:
        """
        Create message list following MessagesState pattern
        
        Args:
            user_query: User's natural language question
            system_prompt: Optional system prompt
            conversation_history: Previous messages in conversation
            
        Returns:
            List of messages for LLM processing
        """
        messages = []
        
        # Add system message if provided
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt))
        
        # Add conversation history
        if conversation_history:
            messages.extend(conversation_history)
        
        # Add current user query
        messages.append(HumanMessage(content=user_query))
        
        return messages
    
    def invoke_with_tools(
        self, 
        messages: List[BaseMessage],
        max_iterations: int = 5
    ) -> Dict[str, Any]:
        """
        Invoke LLM with tools following LangGraph patterns
        
        Args:
            messages: List of messages (MessagesState format)
            max_iterations: Maximum tool calling iterations
            
        Returns:
            Result with messages and tool calls
        """
        try:
            if not self._bound_llm:
                raise ValueError("Bound LLM not available")
            
            # Invoke bound LLM
            response = self._bound_llm.invoke(messages)
            
            # Track tool calls if any
            tool_calls = []
            if hasattr(response, 'tool_calls') and response.tool_calls:
                tool_calls = response.tool_calls
            
            return {
                "response": response,
                "messages": messages + [response],
                "tool_calls": tool_calls,
                "has_tool_calls": len(tool_calls) > 0
            }
            
        except Exception as e:
            return {
                "response": None,
                "messages": messages,
                "tool_calls": [],
                "has_tool_calls": False,
                "error": str(e)
            }
    
    def generate_sql_query(
        self, 
        user_query: str, 
        schema_context: str,
        conversation_history: Optional[List[BaseMessage]] = None
    ) -> Dict[str, Any]:
        """
        Generate SQL query using LangGraph patterns
        
        Args:
            user_query: Natural language question
            schema_context: Database schema information
            conversation_history: Previous conversation messages
            
        Returns:
            SQL generation result
        """
        try:
            # Create enhanced system prompt that emphasizes SUS value mappings
            system_prompt = f"""You are a SQL expert assistant for Brazilian healthcare (SUS) data. Follow SUS database standards EXACTLY.

        Database Schema and Critical Rules:
        {schema_context}
        
        CRITICAL INSTRUCTIONS - READ CAREFULLY:
            1. Generate syntactically correct PostgreSQL queries
            2. Use proper table and column names from the schema above
            3. Handle Portuguese language questions appropriately
            4. Return only the SQL query, no explanation
            5. Use appropriate WHERE clauses for filtering
            6. Include LIMIT clauses when appropriate (default LIMIT 100)
        
        MANDATORY SUS VALUE MAPPINGS - NEVER MAKE MISTAKES:
            - For questions about MEN/HOMENS/MASCULINO: ALWAYS use SEXO = 1
            - For questions about WOMEN/MULHERES/FEMININO: ALWAYS use SEXO = 3
            - For questions about DEATHS/MORTES/ÓBITOS: ALWAYS use MORTE = 1
            - For questions about CITIES/CIDADES: ALWAYS use CIDADE_RESIDENCIA_PACIENTE
        
         EXACT EXAMPLES FOR COMMON QUERIES:
        - "Quantos homens morreram?" → SELECT COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1;
        - "Qual cidade com mais mortes de homens?" → SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY COUNT(*) DESC LIMIT 1;
        - "Mulheres por diagnóstico" → SELECT DIAG_PRINC, COUNT(*) FROM sus_data WHERE SEXO = 3 GROUP BY DIAG_PRINC;
        
        REMEMBER: SEXO values are 1=Male, 3=Female (NOT 2!). Use these values exactly as shown.
        """
            
            # Create messages
            messages = self.create_messages(
                user_query=user_query,
                system_prompt=system_prompt,
                conversation_history=conversation_history
            )
            
            # Invoke with tools
            result = self.invoke_with_tools(messages)
            
            if result.get("error"):
                return {
                    "success": False,
                    "sql_query": None,
                    "error": result["error"],
                    "messages": result["messages"]
                }
            
            # Extract SQL from response or tool calls
            response = result["response"]
            sql_query = ""
            
            # First, try to get SQL from response content
            if hasattr(response, 'content') and response.content:
                sql_query = response.content
            
            # If content is empty but we have tool calls, extract SQL from tool calls
            elif result.get("tool_calls"):
                for tool_call in result["tool_calls"]:
                    if tool_call.get("name") == "sql_db_query":
                        # Extract SQL from tool call arguments
                        sql_query = tool_call.get("args", {}).get("query", "")
                        break
                    elif tool_call.get("name") == "sql_db_query_checker":
                        # Extract SQL from query checker tool
                        sql_query = tool_call.get("args", {}).get("query", "")
                        break
            
            # Clean SQL query
            sql_query = self._clean_sql_query(sql_query)
            
            return {
                "success": True,
                "sql_query": sql_query,
                "error": None,
                "messages": result["messages"],
                "tool_calls": result["tool_calls"]
            }
            
        except Exception as e:
            return {
                "success": False,
                "sql_query": None,
                "error": str(e),
                "messages": []
            }
    
    def generate_conversational_response(
        self, 
        user_query: str,
        context: Optional[str] = None,
        conversation_history: Optional[List[BaseMessage]] = None
    ) -> Dict[str, Any]:
        """
        Generate conversational response using LangGraph patterns
        
        Args:
            user_query: User's question
            context: Additional context (e.g., query results)
            conversation_history: Previous messages
            
        Returns:
            Conversational response result
        """
        try:
            # Create system prompt for conversational response
            system_prompt = f"""You are a helpful assistant for Brazilian healthcare (SUS) data analysis.

        Provide clear, informative responses in Portuguese. Be helpful and accurate.
        
        {f"Context: {context}" if context else ""}
        
        Guidelines:
        1. Answer in Portuguese
        2. Be clear and concise
        3. Use healthcare terminology appropriately
        4. Provide context when explaining medical codes or procedures
        5. Be helpful and informative
        """
            
            # Create messages
            messages = self.create_messages(
                user_query=user_query,
                system_prompt=system_prompt,
                conversation_history=conversation_history
            )
            
            # Invoke LLM (no tools needed for conversational)
            response = self._llm.invoke(messages)
            
            return {
                "success": True,
                "response": response.content if hasattr(response, 'content') else str(response),
                "error": None,
                "messages": messages + [response]
            }
            
        except Exception as e:
            return {
                "success": False,
                "response": f"Erro ao gerar resposta: {str(e)}",
                "error": str(e),
                "messages": []
            }
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """Clean and validate SQL query"""
        if not sql_query:
            return ""
        
        # Remove markdown formatting
        sql_query = sql_query.replace("```sql", "").replace("```", "")

        # Remove comments and extra whitespace
        sql_query = sanitize_sql_for_execution(sql_query)
        
        # Ensure query ends with semicolon
        if not sql_query.strip().endswith(";"):
            sql_query += ";"
        
        return sql_query.strip()
    
    def validate_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate SQL query against database
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Validation result
        """
        try:
            if not self._sql_database or not hasattr(self._sql_database, "_engine"):
                return {
                    "is_valid": False,
                    "error": "Database not initialized",
                    "suggestions": []
                }

            # Detect SQL dialect from SQLAlchemy engine
            engine = self._sql_database._engine
            dialect_name = getattr(getattr(engine, "dialect", None), "name", "") or ""

            # Choose EXPLAIN variant (PostgreSQL)
            explain_prefix = "EXPLAIN"

            # Sanitize SQL to remove comments before validation
            cleaned_sql = sanitize_sql_for_execution(sql_query)

            # Use SQLAlchemy text() for 2.0 compatibility
            from sqlalchemy import text

            # Execute EXPLAIN to validate syntax without running the query
            # Using context manager to ensure connection closure
            explain_sql = f"{explain_prefix} {cleaned_sql}"
            with engine.connect() as connection:
                result = connection.execute(text(explain_sql))
                # Consume results to ensure execution (some drivers require fetch)
                try:
                    result.fetchall()
                except Exception:
                    # Some dialects/queries may not support fetchall on EXPLAIN
                    pass

            return {
                "is_valid": True,
                "error": None,
                "suggestions": []
            }

        except Exception as e:
            # Provide dialect-aware suggestions
            suggestions = [
                "Check table and column names",
                "Verify SQL syntax",
                "Ensure proper WHERE clause formatting",
            ]

            if 'dialect_name' in locals() and dialect_name.lower().startswith("postgres"):
                suggestions.extend([
                    "Quote case-sensitive identifiers with double quotes",
                    "Use ILIKE for case-insensitive matches",
                    "Avoid non-PostgreSQL functions (e.g., strftime)",
                ])

            return {
                "is_valid": False,
                "error": str(e),
                "suggestions": suggestions,
            }
    
    def execute_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Execute SQL query safely
        
        Args:
            sql_query: Valid SQL query to execute
            
        Returns:
            Execution result
        """
        try:
            if not self._sql_database:
                return {
                    "success": False,
                    "results": [],
                    "error": "Database not initialized",
                    "row_count": 0
                }
            
            # Block non-SELECT/unsafe SQL as a second safety layer
            ok, reason = is_select_only(sql_query)
            if not ok:
                return {
                    "success": False,
                    "results": [],
                    "error": f"SQL execution blocked: {reason}",
                    "row_count": 0
                }
            
            # Sanitize SQL to remove comments before execution
            cleaned_sql = sanitize_sql_for_execution(sql_query)

            # Execute query using SQLDatabase
            result = self._sql_database.run(cleaned_sql)
            
            # Parse result (SQLDatabase returns string format)
            if isinstance(result, str):
                # Handle string results from SQLDatabase
                rows = []
                if result.strip():
                    lines = result.strip().split('\n')
                    for line in lines:
                        if line.strip():
                            # Simple parsing for basic results
                            rows.append({"result": line.strip()})
                
                return {
                    "success": True,
                    "results": rows,
                    "error": None,
                    "row_count": len(rows)
                }
            else:
                return {
                    "success": True,
                    "results": result if isinstance(result, list) else [result],
                    "error": None,
                    "row_count": len(result) if isinstance(result, list) else 1
                }
                
        except Exception as e:
            return {
                "success": False,
                "results": [],
                "error": str(e),
                "row_count": 0
            }
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model"""
        try:
            return {
                "provider": self.config.llm_provider,
                "model_name": self.config.llm_model,
                "temperature": self.config.llm_temperature,
                "timeout": self.config.llm_timeout,
                "has_sql_tools": self._sql_toolkit is not None,
                "tools_bound": self._bound_llm is not None,
                "database_connected": self._sql_database is not None,
                "available": True
            }
        except Exception as e:
            return {
                "error": str(e),
                "available": False
            }
    
    def health_check(self) -> Dict[str, Any]:
        """Health check for all components"""
        health = {
            "llm_status": "healthy" if self._llm else "failed",
            "database_status": "healthy" if self._sql_database else "failed",
            "toolkit_status": "healthy" if self._sql_toolkit else "failed",
            "tools_bound": "yes" if self._bound_llm else "no"
        }
        
        overall_status = "healthy" if all(
            status == "healthy" for status in [
                health["llm_status"], 
                health["database_status"], 
                health["toolkit_status"]
            ]
        ) else "degraded"
        
        return {
            "status": overall_status,
            "components": health,
            "model_info": self.get_model_info()
        }


# Factory function for easy instantiation
def create_hybrid_llm_manager(config: ApplicationConfig) -> HybridLLMManager:
    """
    Factory function to create HybridLLMManager
    
    Args:
        config: Application configuration
        
    Returns:
        Configured HybridLLMManager instance
    """
    return HybridLLMManager(config)
