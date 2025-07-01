"""
Query Processing Service - Single Responsibility: Handle all query processing logic
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import time
import re
import logging

from .llm_communication_service import ILLMCommunicationService, LLMResponse
from .database_connection_service import IDatabaseConnectionService
from .schema_introspection_service import ISchemaIntrospectionService
from .error_handling_service import IErrorHandlingService, ErrorCategory


@dataclass
class QueryRequest:
    """Query request with metadata"""
    user_query: str
    session_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class QueryResult:
    """Complete query result with metadata"""
    sql_query: str
    results: List[Dict[str, Any]]
    success: bool
    execution_time: float
    row_count: int
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class QueryValidationResult:
    """Query validation result"""
    is_valid: bool
    is_safe: bool
    warnings: List[str]
    blocked_reasons: List[str]


class IQueryProcessingService(ABC):
    """Interface for query processing"""
    
    @abstractmethod
    def process_natural_language_query(self, request: QueryRequest) -> QueryResult:
        """Process natural language query and return results"""
        pass
    
    @abstractmethod
    def validate_sql_query(self, sql_query: str) -> QueryValidationResult:
        """Validate SQL query for safety and correctness"""
        pass
    
    @abstractmethod
    def execute_sql_query(self, sql_query: str) -> QueryResult:
        """Execute SQL query and return results"""
        pass


class ComprehensiveQueryProcessingService(IQueryProcessingService):
    """Comprehensive query processing implementation using LangChain"""
    
    def __init__(
        self,
        llm_service: ILLMCommunicationService,
        db_service: IDatabaseConnectionService,
        schema_service: ISchemaIntrospectionService,
        error_service: IErrorHandlingService,
        use_langchain_primary: bool = False  # Default to Direct LLM as primary
    ):
        """
        Initialize query processing service
        
        Args:
            llm_service: LLM communication service
            db_service: Database connection service
            schema_service: Schema introspection service
            error_service: Error handling service
            use_langchain_primary: If True, use LangChain as primary (old behavior)
        """
        self._llm_service = llm_service
        self._db_service = db_service
        self._schema_service = schema_service
        self._error_service = error_service
        self._use_langchain_primary = use_langchain_primary
        self._query_history: List[QueryResult] = []
        
        # Setup logging for development
        self._setup_logging()
        
        # Initialize LangChain components
        self._setup_langchain_agent()
    
    def _setup_logging(self) -> None:
        """Setup logging for development visibility"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Only add handler if it doesn't exist to avoid duplicates
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _setup_langchain_agent(self) -> None:
        """Setup LangChain SQL agent with enhanced error handling"""
        try:
            from langchain_community.agent_toolkits.sql.base import create_sql_agent
            from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
            from langchain.agents.agent_types import AgentType
            
            # Get LLM instance (assuming Ollama service)
            if hasattr(self._llm_service, 'get_llm_instance'):
                llm_instance = self._llm_service.get_llm_instance()
            else:
                raise ValueError("LLM service does not provide LangChain-compatible instance")
            
            # Get database connection
            db_connection = self._db_service.get_connection()
            
            # Create SQL toolkit
            self._toolkit = SQLDatabaseToolkit(db=db_connection, llm=llm_instance)
            
            # Create SQL agent with improved configuration
            self._agent = create_sql_agent(
                llm=llm_instance,
                toolkit=self._toolkit,
                agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                verbose=True,
                handle_parsing_errors=True,
                max_iterations=15,  # Increased for complex date queries
                max_execution_time=45  # 45 seconds timeout
            )
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.SYSTEM)
            raise RuntimeError(f"Failed to setup LangChain agent: {error_info.message}")
    
    def process_natural_language_query(self, request: QueryRequest) -> QueryResult:
        """Process natural language query with configurable primary method"""
        start_time = time.time()
        
        try:
            self.logger.info(f"🔍 Processing query: {request.user_query}")
            
            if self._use_langchain_primary:
                # Legacy behavior: LangChain primary, Direct fallback
                try:
                    self.logger.info("🤖 Using LangChain agent as primary method (legacy mode)")
                    return self._process_with_langchain_agent_fallback(request, start_time)
                except Exception as langchain_error:
                    self.logger.warning(f"⚠️ LangChain agent failed: {str(langchain_error)}")
                    self.logger.info("🔄 Attempting direct LLM fallback...")
                    return self._process_with_direct_llm_primary(request, start_time)
            else:
                # New behavior: Direct primary, LangChain fallback
                try:
                    self.logger.info("🎯 Using direct LLM as primary method")
                    return self._process_with_direct_llm_primary(request, start_time)
                except Exception as direct_error:
                    self.logger.warning(f"⚠️ Direct LLM method failed: {str(direct_error)}")
                    self.logger.info("🔄 Attempting LangChain fallback...")
                    return self._process_with_langchain_agent_fallback(request, start_time)
                
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"❌ All processing methods failed: {str(e)}")
            error_info = self._error_service.handle_error(e, ErrorCategory.QUERY_PROCESSING)
            
            query_result = QueryResult(
                sql_query="",
                results=[],
                success=False,
                execution_time=execution_time,
                row_count=0,
                error_message=error_info.message,
                metadata={"error_code": error_info.error_code, "both_methods_failed": True}
            )
            
            self._query_history.append(query_result)
            return query_result
    
    def _process_with_langchain_agent_fallback(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Fallback method: Process query using LangChain SQL Agent"""
        # Get schema context
        schema_context = self._schema_service.get_schema_context()
        self.logger.info("📊 Retrieved schema context for fallback method")
        
        # Create enhanced prompt with schema context
        enhanced_prompt = self._create_enhanced_prompt(request.user_query, schema_context)
        self.logger.info("✨ Created enhanced prompt")
        
        # Process with LangChain agent with timeout
        self.logger.info("🤖 Calling LangChain agent as fallback...")
        agent_response = self._agent.run(enhanced_prompt)
        self.logger.info(f"✅ Agent response received (length: {len(agent_response)})")
        
        # DEBUG: Log full response if it's short or contains issues
        if len(agent_response) < 100 or "error" in agent_response.lower():
            self.logger.warning(f"🔍 DEBUG - Full agent response: {repr(agent_response)}")
        
        # Extract SQL query from response (if available)
        sql_query = self._extract_sql_from_response(agent_response)
        self.logger.info(f"🔧 Extracted SQL: {sql_query[:100]}...")
        
        # Fix case sensitivity issues in SQL query
        sql_query = self._fix_case_sensitivity_issues(sql_query)
        self.logger.info("🛠️ Applied case sensitivity fixes")
        
        # Parse results from agent response
        results, row_count = self._parse_agent_results(agent_response)
        self.logger.info(f"📊 Parsed results: {row_count} rows")
        
        # Check if agent stopped due to iteration limit and try to extract SQL from logs
        if ("Agent stopped due to iteration limit" in agent_response or 
            "Agent stopped due to time limit" in agent_response or
            row_count == 0):
            
            # Try to extract SQL from the response even if execution failed
            if sql_query == "SQL query not found in response":
                # Look for SQL in the chain execution logs
                sql_pattern = r'Action Input:\s*(SELECT\s+COUNT\(\*\)\s+FROM\s+sus_data)\s*;?'
                match = re.search(sql_pattern, agent_response, re.IGNORECASE)
                if match:
                    sql_query = match.group(1).strip()
                    self.logger.info(f"🔧 Extracted SQL from chain logs: {sql_query}")
            
            # If we have SQL but no results, execute directly
            if sql_query != "SQL query not found in response":
                self.logger.info("🔄 Fallback: Executing extracted SQL directly")
                direct_result = self.execute_sql_query(sql_query)
                if direct_result.success:
                    results = direct_result.results
                    row_count = direct_result.row_count
                    self.logger.info("✅ Direct SQL execution successful")
        
        # If the query was fixed for case sensitivity, re-execute the corrected query
        original_sql = self._extract_sql_from_response(agent_response)
        if sql_query != original_sql and sql_query != "SQL query not found in response":
            self.logger.info("🔄 Re-executing corrected query")
            corrected_result = self.execute_sql_query(sql_query)
            if corrected_result.success:
                results = corrected_result.results
                row_count = corrected_result.row_count
                self.logger.info("✅ Corrected query executed successfully")
        
        execution_time = time.time() - start_time
        self.logger.info(f"⏱️ Fallback method completed in {execution_time:.2f}s")
        
        query_result = QueryResult(
            sql_query=sql_query,
            results=results,
            success=True,
            execution_time=execution_time,
            row_count=row_count,
            metadata={
                "agent_response": agent_response,
                "schema_context_used": True,
                "langchain_agent": True,
                "method": "langchain_agent_fallback",
                "method_priority": "fallback"
            }
        )
        
        self._query_history.append(query_result)
        return query_result
    
    def _process_with_direct_llm_primary(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Primary method: Direct LLM call + SQL execution (more reliable)"""
        self.logger.info("🎯 Using direct LLM as primary method")
        
        # Get schema context
        schema_context = self._schema_service.get_schema_context()
        self.logger.info("📊 Retrieved schema context for primary method")
        
        # Create specialized prompt for direct SQL generation
        direct_prompt = self._create_direct_sql_prompt(request.user_query, schema_context)
        self.logger.info("🎨 Created direct SQL prompt")
        
        # Call LLM directly to generate SQL
        llm_response = self._llm_service.send_prompt(direct_prompt)
        self.logger.info(f"🤖 Direct LLM response received (length: {len(llm_response.content)})")
        
        # Extract SQL from LLM response
        sql_query = self._extract_sql_from_direct_response(llm_response.content)
        self.logger.info(f"🔧 Extracted SQL from direct response: {sql_query[:100]}...")
        
        # Fix case sensitivity issues
        sql_query = self._fix_case_sensitivity_issues(sql_query)
        self.logger.info("🛠️ Applied case sensitivity fixes to direct SQL")
        
        # Execute the SQL query directly
        execution_result = self.execute_sql_query(sql_query)
        
        execution_time = time.time() - start_time
        self.logger.info(f"⏱️ Primary method completed in {execution_time:.2f}s")
        
        query_result = QueryResult(
            sql_query=sql_query,
            results=execution_result.results,
            success=execution_result.success,
            execution_time=execution_time,
            row_count=execution_result.row_count,
            error_message=execution_result.error_message,
            metadata={
                "llm_response": llm_response.content,
                "schema_context_used": True,
                "method": "direct_llm_primary",
                "method_priority": "primary"
            }
        )
        
        self._query_history.append(query_result)
        return query_result
    
    # Maintain compatibility with old method names
    def _process_with_direct_llm(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Compatibility wrapper for old method name - redirects to primary method"""
        return self._process_with_direct_llm_primary(request, start_time)
    
    def _process_with_langchain_agent(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Compatibility wrapper for old method name - redirects to fallback method"""
        return self._process_with_langchain_agent_fallback(request, start_time)
    
    def validate_sql_query(self, sql_query: str) -> QueryValidationResult:
        """Validate SQL query for safety and correctness"""
        warnings = []
        blocked_reasons = []
        
        # Basic SQL injection protection
        dangerous_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE",
            "EXEC", "EXECUTE", "xp_", "sp_", "BULK", "OPENROWSET"
        ]
        
        sql_upper = sql_query.upper()
        
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                blocked_reasons.append(f"Palavra-chave perigosa detectada: {keyword}")
        
        # 🚨 CRITICAL: Check for arithmetic date subtraction (incorrect method)
        if 'AVG' in sql_upper and 'DT_SAIDA' in sql_upper and 'DT_INTER' in sql_upper:
            if 'JULIANDAY' not in sql_upper and 'DT_SAIDA - DT_INTER' in sql_upper.replace(' ', ''):
                blocked_reasons.append("❌ Subtração aritmética de datas detectada! Use JULIANDAY para cálculos de tempo corretos.")
                self.logger.error("🚨 BLOCKED: Arithmetic date subtraction detected")
        
        # Check for suspicious patterns
        suspicious_patterns = [
            r"--",  # SQL comments
            r"/\*.*\*/",  # Block comments
            r";.*DROP",  # Multiple statements with DROP
            r";.*DELETE",  # Multiple statements with DELETE
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                warnings.append(f"Padrão suspeito detectado: {pattern}")
        
        # Check for SELECT-only queries (safer)
        if not sql_upper.strip().startswith("SELECT"):
            warnings.append("Consulta não é uma operação SELECT")
        
        # Validate date calculations for hospitalization time
        if 'AVG' in sql_upper and 'DT_SAIDA' in sql_upper and 'DT_INTER' in sql_upper and 'JULIANDAY' in sql_upper:
            warnings.append("✅ Cálculo de data correto com JULIANDAY detectado")
        
        is_safe = len(blocked_reasons) == 0
        is_valid = is_safe and len(warnings) < 3  # Allow some warnings
        
        return QueryValidationResult(
            is_valid=is_valid,
            is_safe=is_safe,
            warnings=warnings,
            blocked_reasons=blocked_reasons
        )
    
    def execute_sql_query(self, sql_query: str) -> QueryResult:
        """Execute SQL query directly (with validation)"""
        start_time = time.time()
        
        try:
            self.logger.info(f"⚡ Executing SQL: {sql_query[:100]}...")
            
            # Validate query first
            validation = self.validate_sql_query(sql_query)
            self.logger.info(f"🔒 Query validation: safe={validation.is_safe}")
            
            if not validation.is_safe:
                raise ValueError(f"Query blocked for safety: {', '.join(validation.blocked_reasons)}")
            
            # Execute query
            conn = self._db_service.get_raw_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            self.logger.info("📊 SQL executed successfully")
            
            # Fetch results
            results = cursor.fetchall()
            column_names = [description[0] for description in cursor.description] if cursor.description else []
            
            # Convert to list of dictionaries
            result_dicts = [dict(zip(column_names, row)) for row in results]
            
            execution_time = time.time() - start_time
            self.logger.info(f"✅ Query returned {len(result_dicts)} rows in {execution_time:.2f}s")
            
            return QueryResult(
                sql_query=sql_query,
                results=result_dicts,
                success=True,
                execution_time=execution_time,
                row_count=len(result_dicts),
                metadata={
                    "validation_warnings": validation.warnings,
                    "direct_execution": True
                }
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"❌ SQL execution failed: {str(e)}")
            error_info = self._error_service.handle_error(e, ErrorCategory.DATABASE)
            
            return QueryResult(
                sql_query=sql_query,
                results=[],
                success=False,
                execution_time=execution_time,
                row_count=0,
                error_message=error_info.message
            )
    
    def _create_enhanced_prompt(self, user_query: str, schema_context) -> str:
        """Create enhanced prompt with schema context"""
        return f"""
{schema_context.formatted_context}

Pergunta do usuário: {user_query}

Por favor, gere e execute uma consulta SQL apropriada para responder esta pergunta.
Seja cuidadoso com nomes de colunas e tipos de dados.
Use as informações do contexto para gerar consultas precisas.

IMPORTANTE - Regras para nomes de cidades:
- Para nomes de cidades (CIDADE_RESIDENCIA_PACIENTE), use sempre a capitalização correta
- Exemplo: CIDADE_RESIDENCIA_PACIENTE = 'Porto Alegre' (não 'porto alegre')
- Se o usuário digitar uma cidade em minúscula, converta para a capitalização correta

IMPORTANTE - Regras para filtros demográficos:
- SEXO = 1 significa masculino/homem
- SEXO = 3 significa feminino/mulher  
- MORTE = 1 significa que o paciente morreu
- MORTE = 0 significa que o paciente não morreu
- Quando perguntarem sobre "homens" use SEXO = 1
- Quando perguntarem sobre "mulheres" use SEXO = 3

IMPORTANTE - Regras para consultas por categoria de doença CID-10:
- SEMPRE use a tabela cid_capitulos para consultas sobre categorias de doenças
- Use o campo categoria_geral para identificar códigos CID de forma precisa
- Para doenças respiratórias: JOIN sus_data com cid_capitulos WHERE c.categoria_geral = 'J'
- Para neoplasias/tumores: JOIN sus_data com cid_capitulos WHERE c.categoria_geral = 'C'
- Para doenças infecciosas: JOIN sus_data com cid_capitulos WHERE c.categoria_geral = 'A'
- Para doenças circulatórias: JOIN sus_data com cid_capitulos WHERE c.categoria_geral = 'I'
- Para doenças digestivas: JOIN sus_data com cid_capitulos WHERE c.categoria_geral = 'K'
- Para qualquer categoria CID: use SEMPRE o padrão:
  SELECT COUNT(*) FROM sus_data s 
  JOIN cid_capitulos c ON s.DIAG_PRINC BETWEEN c.codigo_inicio AND c.codigo_fim 
  WHERE c.categoria_geral = 'LETRA_DO_CID'
- MAPEAMENTO CID-10:
  * J = Doenças respiratórias
  * C = Neoplasias/tumores
  * A = Doenças infecciosas
  * I = Doenças circulatórias
  * K = Doenças digestivas
  * F = Transtornos mentais
  * G = Doenças do sistema nervoso
- VANTAGEM: Usa campo específico categoria_geral, mais preciso que busca textual
- Quando perguntarem sobre TOTAL de casos, NÃO adicione filtros MORTE = 0 a menos que especificamente solicitado
- Quando perguntarem sobre MORTES ou ÓBITOS, SEMPRE adicione AND s.MORTE = 1 na query
- Palavras-chave para mortes: "mortes", "óbitos", "morreram", "faleceram", "deaths"
- Exemplo: "quantas mortes por doenças respiratórias?" → adicionar AND s.MORTE = 1

CRÍTICO - Regras para DATAS (DT_INTER e DT_SAIDA):
- FORMATO OBRIGATÓRIO: As datas são armazenadas como INTEGER no formato YYYYMMDD
- DT_INTER = data de internação, DT_SAIDA = data de saída
- NUNCA use formato de data como '2017-04-01' ou DATE functions
- SEMPRE use números inteiros: 20170401 (para 01/04/2017)

CONVERSÕES DE DATA - LINGUAGEM NATURAL PARA INTEGER:
- "janeiro 2017" = DT_INTER >= 20170101 AND DT_INTER <= 20170131
- "abril 2017" = DT_INTER >= 20170401 AND DT_INTER <= 20170430
- "2017" = DT_INTER >= 20170101 AND DT_INTER <= 20171231
- "entre abril e julho 2017" = DT_INTER >= 20170401 AND DT_INTER <= 20170731
- "primeiro semestre 2020" = DT_INTER >= 20200101 AND DT_INTER <= 20200630

EXEMPLOS CORRETOS DE QUERIES DE DATA:
- "quantos casos em 2017?" → WHERE DT_INTER >= 20170101 AND DT_INTER <= 20171231
- "casos em agosto 2017" → WHERE DT_INTER >= 20170801 AND DT_INTER <= 20170831
- "entre janeiro e março 2020" → WHERE DT_INTER >= 20200101 AND DT_INTER <= 20200331

EXTRAIR ANO/MÊS DE DATAS INTEGER:
- Para extrair ANO: CAST(DT_INTER/10000 AS INTEGER) ou DT_INTER/10000
- Para extrair MÊS: CAST((DT_INTER/100) % 100 AS INTEGER)
- Para agrupar por ano: GROUP BY DT_INTER/10000
- Para agrupar por mês: GROUP BY DT_INTER/100

CRÍTICO - Cálculo de TEMPO DE INTERNAÇÃO:
- TEMPO DE INTERNAÇÃO requer conversão de YYYYMMDD para datas reais
- UTI_MES_TO é APENAS tempo de UTI, NÃO tempo total de internação  
- NUNCA use DT_SAIDA - DT_INTER (subtração aritmética incorreta)
- Use JULIANDAY para conversão correta: 
  JULIANDAY(SUBSTR(DT_SAIDA,1,4)||'-'||SUBSTR(DT_SAIDA,5,2)||'-'||SUBSTR(DT_SAIDA,7,2)) -
  JULIANDAY(SUBSTR(DT_INTER,1,4)||'-'||SUBSTR(DT_INTER,5,2)||'-'||SUBSTR(DT_INTER,7,2))
- UTI_MES_TO = dias específicos em UTI (parte da internação)

EXEMPLO TEMPO MÉDIO CORRETO:
USE JULIANDAY para conversão de datas YYYYMMDD:
SELECT AVG(
    JULIANDAY(SUBSTR(DT_SAIDA,1,4)||'-'||SUBSTR(DT_SAIDA,5,2)||'-'||SUBSTR(DT_SAIDA,7,2)) -
    JULIANDAY(SUBSTR(DT_INTER,1,4)||'-'||SUBSTR(DT_INTER,5,2)||'-'||SUBSTR(DT_INTER,7,2))
) AS tempo_medio_dias FROM sus_data WHERE DIAG_PRINC LIKE 'J%';
Resultado esperado: ~6.1 dias (conversão correta)
INCORRETO: DT_SAIDA - DT_INTER = ~121 dias (subtração aritmética)

NUNCA FAÇA:
❌ DT_INTER BETWEEN '2017-04-01' AND '2017-07-31'
❌ AVG(UTI_MES_TO) para tempo de internação (isso é só UTI!)
❌ UTI_MES_TO como tempo total de internação
❌ strftime('%Y', DT_INTER)
❌ YEAR(DT_INTER)
❌ DATE(DT_INTER)
❌ DATEDIFF function - SQLite não tem
❌ DT_SAIDA - DT_INTER (subtração aritmética incorreta)
❌ // comentarios (use -- para comentários SQL)

SEMPRE FAÇA:
✅ DT_INTER >= 20170401 AND DT_INTER <= 20170731
✅ DT_INTER/10000 = 2017 (para filtrar por ano)
✅ -- comentários SQL (não //)
✅ DT_INTER >= 20170401 (para datas a partir de abril/2017)

IMPORTANTE - Regras para queries COUNT:
- NUNCA adicione LIMIT em queries COUNT(*) - COUNT sempre retorna 1 linha
- Para contar totais: SELECT COUNT(*) FROM... (SEM LIMIT)
- Para listar registros: SELECT * FROM... LIMIT 10 (COM LIMIT)
- Exemplo CORRETO: SELECT COUNT(*) FROM sus_data WHERE...
- Exemplo INCORRETO: SELECT COUNT(*) FROM sus_data WHERE... LIMIT 10
"""
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL query from agent response"""
        # Look for SQL query patterns in the response
        sql_patterns = [
            r"```sql\n(.*?)\n```",
            r"```\n(SELECT.*?)\n```",
            r"Action Input:\s*(SELECT.*?)(?:\n|$)",
            r"sql_db_query\s*Action Input:\s*(SELECT.*?)(?:\n|\r|\r\n|$)",
            r"Action Input:\s*(SELECT.*?)(?:Error:|Observation:|$)",
            r"Action Input:\s*\n?(SELECT.*?)(?:\n|\r|\r\n|Error:|Observation:|$)",
            r"(SELECT COUNT\(\*\) FROM sus_data;?)",
            r"(SELECT.*?FROM sus_data.*?;?)",
            r"(SELECT.*?)(?:\n|$)"
        ]
        
        for pattern in sql_patterns:
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                extracted_sql = match.group(1).strip()
                # Clean up common issues
                extracted_sql = extracted_sql.replace('\n', ' ').replace('\r', ' ')
                extracted_sql = re.sub(r'\s+', ' ', extracted_sql)  # Multiple spaces to single
                # Remove trailing text explanations
                if 'This query' in extracted_sql:
                    extracted_sql = extracted_sql.split('This query')[0].strip()
                return extracted_sql
        
        return "SQL query not found in response"
    
    def _fix_case_sensitivity_issues(self, sql_query: str) -> str:
        """Fix case sensitivity issues in SQL queries"""
        if not sql_query or sql_query == "SQL query not found in response":
            return sql_query
        
        # Fix the pattern: CIDADE_RESIDENCIA_PACIENTE = UPPER('city') or LOWER('city')
        # Convert to: CIDADE_RESIDENCIA_PACIENTE = 'City' (proper case)
        
        # Handle UPPER('city') pattern
        pattern_upper = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*UPPER\s*\(\s*'([^']+)'\s*\)"
        def replacement_upper(match):
            city_name = match.group(1)
            # Convert to proper case (first letter uppercase)
            proper_city = city_name.title()
            return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
        
        fixed_query = re.sub(pattern_upper, replacement_upper, sql_query, flags=re.IGNORECASE)
        
        # Handle LOWER('city') pattern  
        pattern_lower = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*LOWER\s*\(\s*'([^']+)'\s*\)"
        def replacement_lower(match):
            city_name = match.group(1)
            # Convert to proper case (first letter uppercase)
            proper_city = city_name.title()
            return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
        
        fixed_query = re.sub(pattern_lower, replacement_lower, fixed_query, flags=re.IGNORECASE)
        
        # Handle direct lowercase city names: CIDADE_RESIDENCIA_PACIENTE = 'porto alegre'
        pattern_direct = r"CIDADE_RESIDENCIA_PACIENTE\s*=\s*'([a-z][^']*?)'"
        def replacement_direct(match):
            city_name = match.group(1)
            # Convert to proper case only if it's all lowercase
            if city_name.islower():
                proper_city = city_name.title()
                return f"CIDADE_RESIDENCIA_PACIENTE = '{proper_city}'"
            return match.group(0)  # Return original if not all lowercase
        
        fixed_query = re.sub(pattern_direct, replacement_direct, fixed_query)
        
        return fixed_query
    
    def _create_direct_sql_prompt(self, user_query: str, schema_context) -> str:
        """Create optimized prompt for direct SQL generation"""
        return f"""
Você é um especialista em SQL para bases de dados do SUS brasileiro.

CONTEXTO DA BASE DE DADOS:
{schema_context.formatted_context}

PERGUNTA DO USUÁRIO: {user_query}

🚨 REGRA CRÍTICA PARA TEMPO DE INTERNAÇÃO 🚨
SEMPRE use JULIANDAY para calcular diferenças de data!
NUNCA use subtração aritmética direta (DT_SAIDA - DT_INTER)!

❌ INCORRETO (subtração aritmética): DT_SAIDA - DT_INTER
✅ CORRETO (conversão de data): JULIANDAY(...) - JULIANDAY(...)

TEMPLATE OBRIGATÓRIO PARA TEMPO MÉDIO DE INTERNAÇÃO:
SELECT AVG(
    JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
    JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2))
) AS tempo_medio_dias 
FROM sus_data 
WHERE DIAG_PRINC LIKE 'J%';

OUTRAS INSTRUÇÕES:
- Para doenças respiratórias: WHERE DIAG_PRINC LIKE 'J%'
- Para filtros de data: DT_INTER >= 20170401 AND DT_INTER <= 20170430
- Gere APENAS o SQL necessário, sem explicações

SQL:"""
    
    def _extract_sql_from_direct_response(self, response: str) -> str:
        """Extract SQL query from direct LLM response - ENHANCED for JULIANDAY multi-line queries"""
        # Look for SQL after "SQL:" marker
        if "SQL:" in response:
            sql_part = response.split("SQL:")[-1].strip()
        else:
            sql_part = response.strip()
        
        # Remove markdown formatting
        sql_part = sql_part.replace('```sql', '').replace('```', '').strip()
        
        # Split into lines and process
        lines = sql_part.split('\n')
        sql_lines = []
        in_sql_block = False
        
        for line in lines:
            line = line.strip()
            
            # Start collecting SQL when we see SELECT
            if line.upper().startswith('SELECT') or line.upper().startswith('WITH'):
                in_sql_block = True
                sql_lines.append(line)
            elif in_sql_block:
                # Continue collecting until we hit a semicolon or explanatory text
                if line.endswith(';'):
                    sql_lines.append(line)
                    break
                elif line and not line.startswith('--') and not line.startswith('#'):
                    # Check for JULIANDAY patterns - these are valid SQL parts
                    if 'JULIANDAY' in line.upper() or 'SUBSTR' in line.upper() or line.strip().startswith(')'):
                        sql_lines.append(line)
                    # Check for common SQL keywords
                    elif any(keyword in line.upper() for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']):
                        sql_lines.append(line)
                    # Check for table names (sus_data, cid_capitulos, etc.)
                    elif any(table in line.lower() for table in ['sus_data', 'cid_capitulos']):
                        sql_lines.append(line)
                    # Check for parentheses and operators (part of multi-line expressions)
                    elif any(char in line for char in ['(', ')', '+', '-', '*', '/', '||', 'AVG', 'COUNT', 'SUM']):
                        sql_lines.append(line)
                    # Check for simple identifiers that could be column names or values
                    elif line.replace('_', '').replace(' ', '').replace('=', '').replace('1', '').replace('0', '').isalnum() and len(line) < 30:
                        sql_lines.append(line)
                    # Check for conditions like "MORTE = 1" 
                    elif any(pattern in line.upper() for pattern in ['MORTE', 'SEXO', '= 1', '= 3', '= 0']):
                        sql_lines.append(line)
                    # Stop if we hit explanatory text
                    elif any(word in line.lower() for word in ['this query', 'will give', 'para', 'que', 'resultado', 'resposta', 'consulta']):
                        break
                    elif not line and sql_lines:  # Empty line after SQL content
                        break
                elif not line and sql_lines:  # Empty line after SQL content
                    break
        
        # Join the SQL lines and clean up
        if sql_lines:
            full_sql = ' '.join(sql_lines)
            # Clean up extra spaces and formatting
            full_sql = ' '.join(full_sql.split())
            # Ensure it ends with semicolon
            if not full_sql.strip().endswith(';'):
                full_sql += ';'
            
            # Validate we got a JULIANDAY query if it's for hospitalization time
            if 'AVG' in full_sql.upper() and 'DT_SAIDA' in full_sql.upper() and 'DT_INTER' in full_sql.upper():
                if 'JULIANDAY' not in full_sql.upper():
                    # Force JULIANDAY conversion if we detect arithmetic subtraction
                    self.logger.warning("⚠️ Detected arithmetic subtraction, forcing JULIANDAY conversion")
                    full_sql = """SELECT AVG(
                        JULIANDAY(SUBSTR(DT_SAIDA, 1, 4) || '-' || SUBSTR(DT_SAIDA, 5, 2) || '-' || SUBSTR(DT_SAIDA, 7, 2)) -
                        JULIANDAY(SUBSTR(DT_INTER, 1, 4) || '-' || SUBSTR(DT_INTER, 5, 2) || '-' || SUBSTR(DT_INTER, 7, 2))
                    ) AS tempo_medio_dias FROM sus_data WHERE DIAG_PRINC LIKE 'J%';"""
            
            return full_sql
        
        # Fallback: try original single-line extraction
        for line in lines:
            line = line.strip()
            if any(keyword in line.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                clean_line = line.replace('```sql', '').replace('```', '').strip()
                if not clean_line.endswith(';'):
                    clean_line += ';'
                return clean_line
        
        return "SELECT COUNT(*) FROM sus_data;"  # Safe fallback
    
    def _parse_agent_results(self, response: str) -> tuple[List[Dict[str, Any]], int]:
        """Parse results from agent response"""
        # This is a simplified parser - in practice, LangChain agent
        # handles query execution and result formatting
        
        # Look for the SQL query result pattern [(number,)]
        sql_result_pattern = r'\[\((\d+),\)\]'
        sql_match = re.search(sql_result_pattern, response)
        if sql_match:
            result_value = int(sql_match.group(1))
            return [{"result": result_value}], result_value
        
        # Look for Final Answer in the response (with or without colon)
        if "Final Answer:" in response:
            # Extract the final answer part
            final_answer_start = response.find("Final Answer:")
            if final_answer_start != -1:
                final_answer_part = response[final_answer_start + len("Final Answer:"):].strip()
                
                # Check if this is a complex query with multiple results (e.g., top 5 cities)
                # Look for patterns like "1. City - Number, 2. City - Number"
                complex_pattern = r'\d+\. ([\w\s]+) - (\d+)'
                complex_matches = re.findall(complex_pattern, final_answer_part)
                
                if complex_matches:
                    # Complex query with multiple rows - pass complete structured data
                    structured_results = []
                    for rank, (city, count) in enumerate(complex_matches, 1):
                        structured_results.append({
                            "rank": rank,
                            "city": city.strip(),
                            "count": int(count),
                            "full_text": f"{rank}. {city.strip()} - {count}"
                        })
                    
                    # Add the complete final answer text for conversational LLM
                    structured_results.append({
                        "final_answer_text": final_answer_part,
                        "response_type": "complex_query",
                        "total_results": len(complex_matches)
                    })
                    
                    return structured_results, len(complex_matches)
                
                # Check for patterns like "top 5 cities...are City1, City2, City3, City4, and City5"
                # This handles the actual format returned by LangChain
                if "top" in final_answer_part.lower() and "cities" in final_answer_part.lower():
                    # Look for city names in the text
                    cities_pattern = r'are ([^.]+)\.'  # Extract text after "are" and before "."
                    cities_match = re.search(cities_pattern, final_answer_part)
                    
                    if cities_match:
                        cities_text = cities_match.group(1)
                        # Split by commas and "and" to get individual cities
                        cities = re.split(r',\s*(?:and\s+)?', cities_text)
                        cities = [city.strip() for city in cities if city.strip()]
                        
                        # Now extract the actual counts from the agent response
                        # Look for the SQL result pattern in the full response (corrected pattern)
                        sql_result_pattern = r'\[(\([^)]+\)(?:,\s*\([^)]+\))*)\]'
                        sql_match = re.search(sql_result_pattern, response)
                        
                        if sql_match and cities:
                            # Parse the SQL results
                            sql_results_text = sql_match.group(1)
                            # Pattern like ('Uruguaiana', 20), ('Ijuí', 18), etc.
                            city_count_pattern = r"\('([^']+)',\s*(\d+)\)"
                            city_count_matches = re.findall(city_count_pattern, sql_results_text)
                            
                            if city_count_matches:
                                structured_results = []
                                for rank, (city, count) in enumerate(city_count_matches, 1):
                                    structured_results.append({
                                        "rank": rank,
                                        "city": city.strip(),
                                        "count": int(count),
                                        "full_text": f"{rank}. {city.strip()} - {count}"
                                    })
                                
                                # Add the complete final answer text for conversational LLM
                                structured_results.append({
                                    "final_answer_text": final_answer_part,
                                    "response_type": "complex_query",
                                    "total_results": len(city_count_matches),
                                    "sql_results": city_count_matches
                                })
                                
                                return structured_results, len(city_count_matches)
                
                # Simple single number result
                numbers = re.findall(r'\d+', final_answer_part)
                if numbers:
                    result_value = int(numbers[-1])
                    # Include the final answer text for conversational LLM
                    return [
                        {"result": result_value},
                        {"final_answer_text": final_answer_part, "response_type": "simple_query"}
                    ], result_value
        
        # NEW: Handle case where we get just the clean final answer without "Final Answer:" prefix
        # This is what LangChain returns when using .run() method
        
        # First, check if it's a complex multi-line response (e.g., top 5 cities)
        lines = response.strip().split('\n')
        if len(lines) > 1:
            # Look for patterns like "1. City - Number" across multiple lines
            complex_pattern = r'\d+\. ([\w\s]+) - (\d+)'
            all_matches = []
            for line in lines:
                matches = re.findall(complex_pattern, line)
                all_matches.extend(matches)
            
            if all_matches:
                # Complex query with multiple rows - pass complete structured data
                structured_results = []
                for rank, (city, count) in enumerate(all_matches, 1):
                    structured_results.append({
                        "rank": rank,
                        "city": city.strip(),
                        "count": int(count),
                        "full_text": f"{rank}. {city.strip()} - {count}"
                    })
                
                # Add the complete response text for conversational LLM
                structured_results.append({
                    "final_answer_text": response.strip(),
                    "response_type": "complex_query",
                    "total_results": len(all_matches)
                })
                
                return structured_results, len(all_matches)
        
        # Simple single number extraction
        numbers = re.findall(r'\d+', response)
        if numbers:
            result_value = int(numbers[-1])
            # Include the complete response text for conversational LLM
            return [
                {"result": result_value},
                {"final_answer_text": response.strip(), "response_type": "simple_query"}
            ], result_value
        
        # Look for "final answer" without colon
        if "final answer" in response.lower():
            # Find the phrase and extract numbers after it
            final_answer_match = re.search(r'final answer[^0-9]*(\d+)', response, re.IGNORECASE)
            if final_answer_match:
                result_value = int(final_answer_match.group(1))
                return [
                    {"result": result_value},
                    {"final_answer_text": response.strip(), "response_type": "simple_query"}
                ], result_value
        
        # Look for patterns like "result was 308" or just a number at the start
        if "result was" in response.lower():
            # Extract number after "result was"
            match = re.search(r'result was (\d+)', response, re.IGNORECASE)
            if match:
                result_value = int(match.group(1))
                return [
                    {"result": result_value},
                    {"final_answer_text": response.strip(), "response_type": "simple_query"}
                ], result_value
        
        # Look for a number at the beginning of the response (simple case)
        first_line = response.strip().split('\n')[0].strip()
        if first_line.isdigit():
            result_value = int(first_line)
            return [
                {"result": result_value},
                {"final_answer_text": response.strip(), "response_type": "simple_query"}
            ], result_value
        
        # Look for structured results in Observation (fallback)
        if "Observation:" in response:
            # Extract the observation part which usually contains query results
            observation_start = response.find("Observation:")
            if observation_start != -1:
                observation_part = response[observation_start:]
                
                # Try to extract numerical results
                numbers = re.findall(r'\d+', observation_part)
                if numbers:
                    # Simple case: single number result
                    result_value = int(numbers[0])
                    return [
                        {"result": result_value},
                        {"final_answer_text": response.strip(), "response_type": "observation_query"}
                    ], result_value
        
        # Fallback: return complete response text for conversational LLM to interpret
        return [
            {"final_answer_text": response.strip(), "response_type": "fallback_query"}
        ], 0
    
    def get_query_statistics(self) -> Dict[str, Any]:
        """Get query processing statistics"""
        if not self._query_history:
            return {"total_queries": 0}
        
        total_queries = len(self._query_history)
        successful_queries = sum(1 for q in self._query_history if q.success)
        average_execution_time = sum(q.execution_time for q in self._query_history) / total_queries
        
        return {
            "total_queries": total_queries,
            "successful_queries": successful_queries,
            "success_rate": successful_queries / total_queries * 100,
            "average_execution_time": average_execution_time,
            "most_recent_query": self._query_history[-1].sql_query if self._query_history else None
        }


class QueryProcessingFactory:
    """Factory for creating query processing services"""
    
    @staticmethod
    def create_comprehensive_service(
        llm_service: ILLMCommunicationService,
        db_service: IDatabaseConnectionService,
        schema_service: ISchemaIntrospectionService,
        error_service: IErrorHandlingService
    ) -> IQueryProcessingService:
        """Create comprehensive query processing service"""
        return ComprehensiveQueryProcessingService(
            llm_service, db_service, schema_service, error_service
        )
    
    @staticmethod
    def create_service(
        service_type: str,
        llm_service: ILLMCommunicationService,
        db_service: IDatabaseConnectionService,
        schema_service: ISchemaIntrospectionService,
        error_service: IErrorHandlingService
    ) -> IQueryProcessingService:
        """Create query processing service based on type"""
        if service_type.lower() == "comprehensive":
            return ComprehensiveQueryProcessingService(
                llm_service, db_service, schema_service, error_service
            )
        else:
            raise ValueError(f"Unsupported query processing service type: {service_type}")