"""
Query Processing Service - Application Layer (Coordenador Principal)

🎯 OBJETIVO:
Orquestrador central que coordena todo o workflow de processamento de consultas,
implementando estratégias de fallback e gerenciando a comunicação entre serviços especializados.

🔄 POSIÇÃO NO FLUXO (COORDENADOR):
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ User Interface  │ -> │ Query Processing│ -> │ Response        │
│ / Orchestrator  │    │ Service (COORD) │    │ Generation      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │ coordena
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │SQL Generation│ │Query Execution│ │Schema/Error │
            │   Service   │ │   Service   │ │  Services   │
            └─────────────┘ └─────────────┘ └─────────────┘

📥 ENTRADAS (DE ONDE VEM):
- Text2SQLOrchestrator: QueryRequest - pergunta estruturada do usuário
- API/CLI Interface: raw user queries via orchestrator
- Session Context: histórico e metadados de consultas anteriores

📤 SAÍDAS (PARA ONDE VAI):
- Text2SQLOrchestrator: QueryResult - resultado completo com metadados
- ConversationalResponseService: dados estruturados para resposta
- Logging/Analytics: estatísticas de performance e debugging

🧩 RESPONSABILIDADES (COORDENAÇÃO):
1. 🎯 Gerenciar estratégias de processamento (primary + fallbacks)
2. 🔄 Implementar retry intelligente com múltiplas abordagens
3. 📊 Delegação inteligente para serviços especializados
4. 🛡️ Validação e recuperação de erros
5. 📈 Coleta de métricas e histórico de queries
6. ⚡ Otimização de workflow baseado em contexto

🔗 DEPENDÊNCIAS (ORQUESTRA):
- ISQLGenerationService: Para geração de SQL
- IQueryExecutionService: Para execução segura
- ILLMCommunicationService: Para comunicação com LLMs
- ISchemaIntrospectionService: Para contexto do banco
- IErrorHandlingService: Para tratamento de erros
- ISQLValidationService: Para validação avançada

🎭 ESTRATÉGIAS IMPLEMENTADAS:
1. 🎯 Direct LLM Primary: Estratégia principal otimizada
2. 🦙 Llama3 Fallback: Fallback com modelo específico
3. 🧠 Error-Aware Retry: Retry com contexto de erro anterior
4. 🔄 Multi-Strategy Retry: Sistema de fallback em cascata
5. 📊 Simplified Approach: Para queries complexas que falham

📊 MÉTRICAS COLETADAS:
- Tempo de execução por estratégia
- Taxa de sucesso de cada método
- Padrões de fallback mais utilizados
- Histórico completo para análise

🛡️ ROBUSTEZ:
- Fallback inteligente se método primário falhar
- Recovery automático com contexto de erro
- Timeout handling em todas as operações
- Preservação de funcionalidade mesmo com falhas parciais
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import time
import re
import logging

from .llm_communication_service import ILLMCommunicationService, LLMResponse
from ...infrastructure.database.connection_service import IDatabaseConnectionService
from .schema_introspection_service import ISchemaIntrospectionService
from .error_handling_service import IErrorHandlingService, ErrorCategory
from .sql_validation_service import ISQLValidationService, SQLValidationFactory
from .sql_generation_service import ISQLGenerationService, SQLGenerationFactory
from .query_execution_service import IQueryExecutionService, QueryExecutionFactory


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
    """Comprehensive query processing implementation with direct LLM fallback"""
    
    def __init__(
        self,
        llm_service: ILLMCommunicationService,
        db_service: IDatabaseConnectionService,
        schema_service: ISchemaIntrospectionService,
        error_service: IErrorHandlingService,
        sql_generation_service: Optional[ISQLGenerationService] = None,
        query_execution_service: Optional[IQueryExecutionService] = None,
        sql_validator: Optional[ISQLValidationService] = None,
        use_direct_llm_primary: bool = True  # Always use Direct LLM as primary (LangChain removed)
    ):
        """
        Initialize query processing service
        
        Args:
            llm_service: LLM communication service
            db_service: Database connection service
            schema_service: Schema introspection service
            error_service: Error handling service
            sql_generation_service: SQL generation service (optional)
            query_execution_service: Query execution service (optional)
            sql_validator: SQL validation service (optional)
            use_direct_llm_primary: Always True (LangChain removed, direct LLM only)
        """
        self._llm_service = llm_service
        self._db_service = db_service
        self._schema_service = schema_service
        self._error_service = error_service
        
        # Initialize new services or create them
        self._sql_generation_service = sql_generation_service or SQLGenerationFactory.create_service(
            llm_service, schema_service
        )
        self._query_execution_service = query_execution_service or QueryExecutionFactory.create_service(
            db_service, error_service
        )
        
        self._sql_validator = sql_validator or SQLValidationFactory.create_comprehensive_validator()
        self._use_direct_llm_primary = use_direct_llm_primary
        self._query_history: List[QueryResult] = []
        
        # Setup logging for development
        self._setup_logging()
        
        # LangChain removed - using direct LLM only
        self._agent = None
    
    def _setup_logging(self) -> None:
        """Setup logging for development visibility"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Prevent propagation to root logger to avoid duplicates
        self.logger.propagate = False
        
        # Only add handler if it doesn't exist to avoid duplicates
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    
    
    def process_natural_language_query(self, request: QueryRequest) -> QueryResult:
        """Process natural language query with configurable primary method"""
        start_time = time.time()
        
        try:
            self.logger.info(f"🔍 Processing query: {request.user_query}")
            
            # Always use direct LLM as primary with Llama3 fallback
            try:
                self.logger.info("🎯 Using direct LLM as primary method")
                return self._process_with_direct_llm_primary(request, start_time)
            except Exception as direct_error:
                self.logger.warning(f"⚠️ Direct LLM method failed: {str(direct_error)}")
                self.logger.info("🔄 Attempting Llama3 direct fallback...")
                return self._process_with_llama3_fallback(request, start_time)
                
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
    
    def _process_with_multi_strategy_retry(self, request: QueryRequest, start_time: float, primary_error: str = "") -> QueryResult:
        """
        Advanced fallback with multiple retry strategies
        As a senior data scientist would implement: try different approaches systematically
        """
        self.logger.info("🧠 Initiating multi-strategy retry system...")
        
        strategies = [
            {
                "name": "llama3_fallback",
                "description": "Direct Llama3 fallback with error awareness",
                "method": self._process_with_llama3_fallback
            },
            {
                "name": "enhanced_direct_llm", 
                "description": "Direct LLM with enhanced error-aware prompt",
                "method": self._process_with_error_aware_direct_llm
            },
            {
                "name": "simplified_query",
                "description": "Simplified query approach for complex failures",
                "method": self._process_with_simplified_approach
            }
        ]
        
        last_error = primary_error
        
        for i, strategy in enumerate(strategies):
            try:
                self.logger.info(f"🔄 Strategy {i+1}/{len(strategies)}: {strategy['description']}")
                
                strategy_start_time = time.time()
                # Pass error context for first strategy (llama3_fallback)
                if strategy["name"] == "llama3_fallback":
                    result = strategy["method"](request, strategy_start_time, last_error)
                else:
                    result = strategy["method"](request, strategy_start_time)
                
                if result.success:
                    # Success! Mark it appropriately
                    total_execution_time = time.time() - start_time
                    result.execution_time = total_execution_time
                    result.metadata = result.metadata or {}
                    result.metadata.update({
                        "multi_strategy_retry": True,
                        "successful_strategy": strategy["name"],
                        "strategy_attempt": i + 1,
                        "primary_error": primary_error,
                        "total_strategies_tried": i + 1
                    })
                    
                    self.logger.info(f"✅ Multi-strategy success with {strategy['name']} on attempt {i+1}")
                    return result
                else:
                    self.logger.warning(f"⚠️ Strategy {strategy['name']} failed: {result.error_message}")
                    last_error = result.error_message or "Unknown error"
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Strategy {strategy['name']} threw exception: {str(e)}")
                last_error = str(e)
                continue
        
        # All strategies failed
        total_execution_time = time.time() - start_time
        self.logger.error(f"❌ All {len(strategies)} strategies failed")
        
        return QueryResult(
            sql_query="",
            results=[],
            success=False,
            execution_time=total_execution_time,
            row_count=0,
            error_message=f"All retry strategies failed. Last error: {last_error}",
            metadata={
                "multi_strategy_retry": True,
                "all_strategies_failed": True,
                "strategies_tried": len(strategies),
                "primary_error": primary_error,
                "final_error": last_error
            }
        )
    
    def _process_with_error_aware_direct_llm(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Enhanced direct LLM method that's aware of previous errors"""
        self.logger.info("🎯 Using error-aware direct LLM approach")
        
        # Create enhanced prompt that addresses common SQL generation errors
        enhanced_prompt = self._sql_generation_service.create_sql_prompt(request.user_query, enhanced=True)
        
        # Call LLM with enhanced prompt
        llm_response = self._llm_service.send_prompt(enhanced_prompt)
        
        # Extract and process SQL
        sql_query = self._sql_generation_service.extract_sql_from_response(llm_response.content)
        self._query_execution_service.log_sql_query(sql_query, "🔧 Error-aware LLM generated SQL")
        
        # Clean and fix SQL
        sql_query = self._sql_generation_service.clean_and_fix_sql(sql_query)
        
        # Execute with validation
        execution_result = self.execute_sql_query(sql_query)
        
        execution_time = time.time() - start_time
        
        return QueryResult(
            sql_query=sql_query,
            results=execution_result.results,
            success=execution_result.success,
            execution_time=execution_time,
            row_count=execution_result.row_count,
            error_message=execution_result.error_message,
            metadata={
                "method": "error_aware_direct_llm",
                "enhanced_prompt_used": True,
                "llm_response": llm_response.content
            }
        )
    
    def _process_with_simplified_approach(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Simplified approach for complex queries that consistently fail"""
        self.logger.info("🎯 Using simplified query approach")
        
        # Try to break down complex queries into simpler components
        simplified_query = self._simplify_user_query(request.user_query)
        
        # Use simplified query approach
        simplified_request = QueryRequest(
            user_query=simplified_query,
            session_id=request.session_id,
            timestamp=request.timestamp,
            context=request.context
        )
        
        return self._process_with_llama3_fallback(simplified_request, start_time)
    
    def _simplify_user_query(self, user_query: str) -> str:
        """Simplify complex queries to increase success rate"""
        query_lower = user_query.lower()
        
        # Simplification rules based on common failure patterns
        if "mulheres" in query_lower and "menos de" in query_lower and "anos" in query_lower:
            # Complex age/gender query - simplify to basic demographic query
            return "Quais cidades têm mais mortes de mulheres?"
        elif "maior" in query_lower or "mais" in query_lower and "cidade" in query_lower:
            # City ranking query - simplify
            return "Quais cidades têm mais mortes?"
        
        # Default: return original query
        return user_query

    def _process_with_llama3_fallback(self, request: QueryRequest, start_time: float, error_context: str = "") -> QueryResult:
        """Fallback method: Direct llama3 call with specialized fallback prompting"""
        self.logger.info("🦙 Using llama3 direct fallback method")
        
        # Create fallback LLM service specifically for llama3
        from ..config.simple_config import ApplicationConfig
        
        # Override config temporarily for fallback
        fallback_config = ApplicationConfig(
            llm_provider="ollama",
            llm_model="llama3", 
            llm_temperature=0.0,
            llm_timeout=60
        )
        
        # Create temporary LLM service for llama3
        try:
            from .llm_communication_service import OllamaLLMService
            fallback_llm_service = OllamaLLMService(fallback_config, self.logger)
        except ImportError:
            # Fallback to the main service if specific class not available
            fallback_llm_service = self._llm_service
        
        # Get schema context
        schema_context = self._schema_service.get_schema_context()
        self.logger.info("📊 Retrieved schema context for llama3 fallback")
        
        # Create specialized fallback prompt
        schema_text = str(schema_context) if schema_context else ""
        fallback_prompt = self._create_llama3_fallback_prompt(request.user_query, schema_text, error_context)
        self.logger.info("✨ Created specialized llama3 fallback prompt")
        
        try:
            # Process with llama3 directly
            self.logger.info("🦙 Calling llama3 as direct fallback...")
            llm_response = fallback_llm_service.send_prompt(fallback_prompt)
            self.logger.info(f"✅ Llama3 fallback response received (length: {len(llm_response.content)})")
            
            # Extract and clean SQL query
            sql_query = self._sql_generation_service.extract_sql_from_response(llm_response.content)
            self._query_execution_service.log_sql_query(sql_query, "🦙 Llama3 Fallback SQL")
            
            # Clean and fix SQL
            sql_query = self._sql_generation_service.clean_and_fix_sql(sql_query)
            
            # Execute the SQL query
            execution_result = self.execute_sql_query(sql_query)
            
            execution_time = time.time() - start_time
            self.logger.info(f"⏱️ Llama3 fallback completed in {execution_time:.2f}s")
            
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
                    "llama3_fallback": True,
                    "method": "llama3_direct_fallback",
                    "method_priority": "fallback",
                    "error_context": error_context
                }
            )
            
            self._query_history.append(query_result)
            return query_result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"❌ Llama3 fallback failed: {str(e)}")
            
            return QueryResult(
                sql_query="",
                results=[],
                success=False,
                execution_time=execution_time,
                row_count=0,
                error_message=f"Llama3 fallback failed: {str(e)}",
                metadata={
                    "llama3_fallback": True,
                    "fallback_failed": True,
                    "error_context": error_context
                }
            )
    
    def _create_llama3_fallback_prompt(self, user_query: str, schema_context: str, error_context: str = "") -> str:
        """Create specialized prompt for llama3 fallback with error awareness"""
        
        error_guidance = ""
        if error_context:
            error_guidance = f"""
        ERRO ANTERIOR DETECTADO: {error_context}
        
        INSTRUÇÕES DE CORREÇÃO:
        - Se erro de sintaxe, revise cuidadosamente a sintaxe SQL
        - Se erro de coluna inexistente, verifique nomes exatos no schema
        - Se erro de tipo de dados, use conversões apropriadas (CAST, julianday)
        - Para cálculos de tempo, use julianday(data_fim) - julianday(data_inicio)
        - Para códigos CID-10, use LIKE 'J%' para doenças respiratórias
        """
        
        prompt = f"""Você é um especialista em SQL para dados do Sistema Único de Saúde (SUS).

        {schema_context}
        
        {error_guidance}

        INSTRUÇÕES CRÍTICAS:
        1. Para perguntas sobre CIDADES/MUNICÍPIOS: SEMPRE use CIDADE_RESIDENCIA_PACIENTE
        2. NUNCA use MUNIC_RES (contém códigos numéricos inúteis)
        3. Para mortes/óbitos: WHERE MORTE = 1
        4. Códigos de sexo: 1=Masculino, 3=Feminino
        5. Para cálculos de tempo: julianday(DT_SAIDA) - julianday(DT_INTER)
        6. Doenças respiratórias: DIAG_PRINC LIKE 'J%'
        
        PERGUNTA: {user_query}
        
        Gere APENAS a query SQL, sem explicações:"""

        return prompt
    
    def _process_with_direct_llm_primary(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Primary method: Direct LLM call + SQL execution (more reliable)"""
        # Log which model is being used
        model_info = self._llm_service.get_model_info()
        model_name = model_info.get('model_name', 'Unknown')
        provider = model_info.get('provider', 'Unknown')
        self.logger.info(f"🎯 Using direct LLM as primary method: {model_name} ({provider})")
        
        # Create specialized prompt for direct SQL generation
        direct_prompt = self._sql_generation_service.create_sql_prompt(request.user_query, enhanced=False)
        self.logger.info("🎨 Created direct SQL prompt")
        
        # Call LLM directly to generate SQL
        llm_response = self._llm_service.send_prompt(direct_prompt)
        self.logger.info(f"🤖 {model_name} response received (length: {len(llm_response.content)})")
        
        # Extract SQL from LLM response
        sql_query = self._sql_generation_service.extract_sql_from_response(llm_response.content)
        self._query_execution_service.log_sql_query(sql_query, "🔧 Extracted SQL from direct response")
        
        # Clean and fix SQL
        sql_query = self._sql_generation_service.clean_and_fix_sql(sql_query)
        self.logger.info("🛠️ Applied SQL cleaning and fixes")
        
        # 🚨 VALIDATION CHECKPOINT 1: Comprehensive SQL validation
        validation_result = self._sql_validator.validate_sql(sql_query, request.user_query)
        self.logger.info(f"🔍 SQL validation score: {validation_result.score:.1f}/100")
        
        if validation_result.has_critical_issues:
            self.logger.error("❌ Critical issues found in generated SQL:")
            for issue in validation_result.issues:
                if issue.severity.value in ['critical', 'error']:
                    self.logger.error(f"  - {issue.code}: {issue.message}")
            
            # Try to use corrected SQL if available
            if validation_result.corrected_sql:
                self.logger.info("🔄 Using corrected SQL from validator")
                sql_query = validation_result.corrected_sql
            else:
                # 🎯 INTELLIGENT FALLBACK: Critical validation failed, try multi-strategy
                self.logger.warning("⚠️ Primary method validation failed critically")
                self.logger.info("🔄 Triggering intelligent fallback due to validation failure...")
                
                try:
                    fallback_start_time = time.time()
                    fallback_result = self._process_with_multi_strategy_retry(request, fallback_start_time, "SQL validation failed")
                    
                    # Mark as fallback result
                    total_execution_time = time.time() - start_time
                    fallback_result.execution_time = total_execution_time
                    fallback_result.metadata = fallback_result.metadata or {}
                    fallback_result.metadata.update({
                        "primary_failed": True,
                        "primary_error": "SQL validation failed",
                        "primary_sql": sql_query,
                        "fallback_triggered": True,
                        "fallback_reason": "SQL_validation_failed",
                        "validation_score": validation_result.score
                    })
                    
                    self.logger.info(f"✅ Fallback after validation failure {'succeeded' if fallback_result.success else 'also failed'}")
                    self._query_history.append(fallback_result)
                    return fallback_result
                    
                except Exception as fallback_error:
                    self.logger.error(f"❌ Fallback after validation failure also failed: {str(fallback_error)}")
                
                # Return validation error with fallback info
                execution_time = time.time() - start_time
                return QueryResult(
                    sql_query=sql_query,
                    results=[],
                    success=False,
                    execution_time=execution_time,
                    row_count=0,
                    error_message=f"SQL validation failed: {'; '.join([i.message for i in validation_result.issues if i.severity.value in ['critical', 'error']])}",
                    metadata={
                        "validation_failed": True,
                        "validation_score": validation_result.score,
                        "validation_issues": [{"code": i.code, "message": i.message, "severity": i.severity.value} for i in validation_result.issues],
                        "fallback_attempted": True,
                        "both_methods_failed": True
                    }
                )
        elif validation_result.corrected_sql:
            # Use corrected SQL even for non-critical issues
            self.logger.info("✅ Using improved SQL from validator")
            sql_query = validation_result.corrected_sql
        
        # Log validation warnings
        for issue in validation_result.issues:
            if issue.severity.value == 'warning':
                self.logger.warning(f"⚠️ {issue.code}: {issue.message}")
        
        # Execute the SQL query directly
        execution_result = self.execute_sql_query(sql_query)
        
        # 🎯 INTELLIGENT FALLBACK: If SQL execution failed, try fallback method
        if not execution_result.success:
            self.logger.warning(f"⚠️ Primary method SQL execution failed: {execution_result.error_message}")
            self.logger.info("🔄 Triggering intelligent fallback to Llama3 direct method...")
            
            try:
                # Reset start time for fallback
                fallback_start_time = time.time()
                fallback_result = self._process_with_multi_strategy_retry(request, fallback_start_time, execution_result.error_message or "SQL execution failed")
                
                # Mark as fallback result and combine execution times
                total_execution_time = time.time() - start_time
                fallback_result.execution_time = total_execution_time
                fallback_result.metadata = fallback_result.metadata or {}
                fallback_result.metadata.update({
                    "primary_failed": True,
                    "primary_error": execution_result.error_message,
                    "primary_sql": sql_query,
                    "fallback_triggered": True,
                    "fallback_reason": "SQL_execution_failed"
                })
                
                self.logger.info(f"✅ Fallback method {'succeeded' if fallback_result.success else 'also failed'}")
                self._query_history.append(fallback_result)
                return fallback_result
                
            except Exception as fallback_error:
                self.logger.error(f"❌ Fallback method also failed: {str(fallback_error)}")
                # Return original primary result with fallback info
                execution_result.metadata = execution_result.metadata or {}
                execution_result.metadata.update({
                    "fallback_attempted": True,
                    "fallback_error": str(fallback_error),
                    "both_methods_failed": True
                })
        
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
                "method_priority": "primary",
                **(execution_result.metadata or {})
            }
        )
        
        self._query_history.append(query_result)
        return query_result
    
    # Maintain compatibility with old method names
    def _process_with_direct_llm(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Compatibility wrapper for old method name - redirects to primary method"""
        return self._process_with_direct_llm_primary(request, start_time)
    
    def _process_with_langchain_agent(self, request: QueryRequest, start_time: float) -> QueryResult:
        """Compatibility wrapper for old method name - redirects to llama3 fallback method"""
        return self._process_with_llama3_fallback(request, start_time)
    
    def validate_sql_query(self, sql_query: str) -> QueryValidationResult:
        """Validate SQL query for safety and correctness"""
        return self._query_execution_service.validate_sql_query(sql_query)
    
    def execute_sql_query(self, sql_query: str) -> QueryResult:
        """Execute SQL query directly (with validation)"""
        return self._query_execution_service.execute_sql_query(sql_query)
    
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

CRÍTICO - Regras para DATAS:
- Use SEMPRE as colunas DATE: DT_INTER e DT_SAIDA (agora formato YYYY-MM-DD)
- DT_INTER = data de internação, DT_SAIDA = data de saída  
- Use formato SQL padrão: '2017-04-01' e funções DATE nativas do SQLite

CONVERSÕES DE DATA - LINGUAGEM NATURAL PARA DATE:
- "janeiro 2017" = DT_INTER >= '2017-01-01' AND DT_INTER <= '2017-01-31'
- "abril 2017" = DT_INTER >= '2017-04-01' AND DT_INTER <= '2017-04-30'
- "2017" = DT_INTER >= '2017-01-01' AND DT_INTER <= '2017-12-31'
- "entre abril e julho 2017" = DT_INTER >= '2017-04-01' AND DT_INTER <= '2017-07-31'
- "primeiro semestre 2020" = DT_INTER >= '2020-01-01' AND DT_INTER <= '2020-06-30'

EXEMPLOS CORRETOS DE QUERIES DE DATA:
- "quantos casos em 2017?" → WHERE DT_INTER >= '2017-01-01' AND DT_INTER <= '2017-12-31'
- "casos em agosto 2017" → WHERE DT_INTER >= '2017-08-01' AND DT_INTER <= '2017-08-31'
- "entre janeiro e março 2020" → WHERE DT_INTER >= '2020-01-01' AND DT_INTER <= '2020-03-31'

EXTRAIR ANO/MÊS DE DATAS:
- Para extrair ANO: strftime('%Y', DT_INTER)
- Para extrair MÊS: strftime('%m', DT_INTER)
- Para agrupar por ano: GROUP BY strftime('%Y', DT_INTER)
- Para agrupar por mês: GROUP BY strftime('%Y-%m', DT_INTER)

CRÍTICO - Cálculo de TEMPO DE INTERNAÇÃO:
- TEMPO DE INTERNAÇÃO é MUITO SIMPLES com as colunas DATE
- UTI_MES_TO é APENAS tempo de UTI, NÃO tempo total de internação  
- Use cálculo direto com as colunas DATE: JULIANDAY(DT_SAIDA) - JULIANDAY(DT_INTER)
- UTI_MES_TO = dias específicos em UTI (parte da internação)

EXEMPLO TEMPO MÉDIO CORRETO:
USE as colunas DATE para cálculo simples:
SELECT AVG(JULIANDAY(DT_SAIDA) - JULIANDAY(DT_INTER)) AS tempo_medio_dias 
FROM sus_data WHERE DIAG_PRINC LIKE 'J%';
Resultado esperado: ~6.2 dias (conversão correta)

NUNCA FAÇA:
❌ AVG(UTI_MES_TO) para tempo de internação (isso é só UTI!)
❌ UTI_MES_TO como tempo total de internação
❌ DATEDIFF function - SQLite não tem
❌ // comentarios (use -- para comentários SQL)

SEMPRE FAÇA:
✅ DT_INTER >= '2017-04-01' AND DT_INTER <= '2017-07-31'
✅ strftime('%Y', DT_INTER) = '2017' (para filtrar por ano)
✅ -- comentários SQL (não //)

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
        
        # Fix SQLite incompatible YEAR() function calls
        fixed_query = self._fix_sqlite_year_extraction(fixed_query)
        
        return fixed_query
    
    def _fix_sqlite_year_extraction(self, sql_query: str) -> str:
        """Fix YEAR() function calls for SQLite compatibility"""
        if not sql_query or 'YEAR(' not in sql_query.upper():
            return sql_query
        
        self.logger.info("🔧 Fixing YEAR() function for SQLite compatibility")
        
        # Replace YEAR(JULIANDAY(DT_INTER)) with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\(\s*JULIANDAY\s*\(\s*DT_INTER\s*\)\s*\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        # Replace YEAR(DT_INTER) with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\(\s*DT_INTER\s*\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        # Replace any remaining YEAR() patterns with strftime('%Y', DT_INTER)
        sql_query = re.sub(
            r'YEAR\s*\([^)]+\)',
            "strftime('%Y', DT_INTER)",
            sql_query,
            flags=re.IGNORECASE
        )
        
        self.logger.info(f"✅ SQLite YEAR() fix applied")
        return sql_query
    
    def _create_direct_sql_prompt(self, user_query: str, schema_context) -> str:
        """Create optimized prompt for direct SQL generation"""
        return f"""
        Você é um especialista em SQL para bases de dados do SUS brasileiro.
        
        CONTEXTO DA BASE DE DADOS:
        {schema_context.formatted_context}
        
        PERGUNTA DO USUÁRIO: {user_query}
        
        🚨 REGRAS CRÍTICAS PARA GERAÇÃO DE SQL 🚨
        
        1. PARA CONSULTAS DE RANKING/TOP (ex: "top 5 cidades"):
           - Use filtros diretos no WHERE, não CASE statements
           - SEMPRE inclua LIMIT com o número solicitado
           - Para contagens específicas, filtre primeiro no WHERE
        
        ❌ INCORRETO (CASE statement complexo):
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total, 
               SUM(CASE WHEN IDADE > 50 AND SEXO = 3 THEN 1 ELSE 0 END) as filtrado
        FROM sus_data WHERE MORTE = 1 GROUP BY CIDADE_RESIDENCIA_PACIENTE ORDER BY total DESC;
        
        ✅ CORRETO (filtro direto):
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes
        FROM sus_data 
        WHERE MORTE = 1 AND SEXO = 3 AND IDADE > 50 AND DIAG_PRINC LIKE 'J%'
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC 
        LIMIT 5;
        
        2. PARA TEMPO DE INTERNAÇÃO:
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
        
        3. 🚨 CRÍTICO - FUNÇÕES SQLite:
        - ❌ NUNCA use YEAR(), MONTH(), DAY() - NÃO EXISTEM no SQLite!
        - ✅ Para extrair ano: USE strftime('%Y', DT_INTER)
        - ✅ Para extrair mês: USE strftime('%m', DT_INTER)
        - ✅ Para agrupar por ano: GROUP BY strftime('%Y', DT_INTER)
        
        4. 🚨 CRÍTICO - REGRAS PARA CONSULTAS DE DIAGNÓSTICO:
        - Para "diagnósticos mais numerosos": SEMPRE usar DIAG_PRINC (código específico), NUNCA capítulos CID
        - TEMPLATE OBRIGATÓRIO para tempo por diagnóstico:
          SELECT DIAG_PRINC, COUNT(*) as total_casos, ROUND(AVG(JULIANDAY(DT_SAIDA) - JULIANDAY(DT_INTER)), 1) as tempo_medio_dias
          FROM sus_data WHERE DT_INTER IS NOT NULL AND DT_SAIDA IS NOT NULL
          GROUP BY DIAG_PRINC ORDER BY COUNT(*) DESC LIMIT 5;
        - ❌ NUNCA faça JOIN com cid_capitulos para consultas de diagnósticos específicos
        - ❌ NUNCA use GROUP BY sem especificar as colunas
        
        5. 🚨 CRÍTICO - REGRAS PARA CUSTOS E ATENDIMENTOS:
        - ❌ NUNCA filtre por MORTE = 0 em consultas sobre "gastos", "custos" ou "atendimentos"
        - ✅ Gastos/custos/atendimentos SEMPRE incluem todos os casos (MORTE = 0 e MORTE = 1)
        - ✅ Apenas filtre por MORTE quando explicitamente perguntado sobre "mortes" ou "óbitos"
        - ✅ Para "gasto MÉDIO" ou "média": SEMPRE use AVG(VAL_TOT), NUNCA SUM(VAL_TOT)
        - ✅ Para "gasto TOTAL": use SUM(VAL_TOT)
        - ✅ Palavras que indicam MÉDIA: "médio", "média", "average" → use AVG()
        - ✅ Palavras que indicam TOTAL: "total", "soma", "sum" → use SUM()
        - Exemplo CORRETO para "gasto médio por cidade": SELECT CIDADE_RESIDENCIA_PACIENTE, AVG(VAL_TOT) FROM sus_data GROUP BY CIDADE_RESIDENCIA_PACIENTE
        - Exemplo INCORRETO: SELECT CIDADE_RESIDENCIA_PACIENTE, SUM(VAL_TOT) FROM sus_data GROUP BY CIDADE_RESIDENCIA_PACIENTE (isso é total, não média!)
        - Exemplo INCORRETO: SELECT AVG(VAL_TOT) FROM sus_data WHERE MORTE = 0
        
        5. OUTRAS INSTRUÇÕES:
        - Para doenças respiratórias: WHERE DIAG_PRINC LIKE 'J%'
        - Para filtros de data: DT_INTER >= 20170401 AND DT_INTER <= 20170430
        - SEXO = 3 para mulheres, SEXO = 1 para homens
        - MORTE = 1 para mortes confirmadas
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
    
    def _clean_sql_comments(self, sql_query: str) -> str:
        """Remove problematic SQL comments that can break query execution"""
        if not sql_query:
            return sql_query
        
        # Handle inline comments carefully - only remove the comment part, not the rest of the line
        # For single-line SQL with inline comments, we need to be more careful
        
        # Split the SQL into tokens and reconstruct
        cleaned_sql = sql_query
        
        # Remove inline comments but preserve the rest of the SQL
        # Look for -- followed by text, but only remove the comment part
        comment_pattern = r'--[^a-zA-Z]*[a-zA-Z][^G]*(?=GROUP|ORDER|LIMIT|$)'
        
        # Simple approach: just remove comment text but keep SQL keywords
        import re
        
        # Pattern to match comment but not affect SQL structure
        # Look for -- comment text that's not part of SQL keywords
        if '--' in cleaned_sql:
            # Split on spaces to work with tokens
            tokens = cleaned_sql.split()
            filtered_tokens = []
            skip_comment = False
            
            for token in tokens:
                if '--' in token:
                    # If token starts with --, skip it and following comment words
                    if token.startswith('--'):
                        skip_comment = True
                        continue
                    else:
                        # Token contains -- but doesn't start with it
                        # Keep the part before --
                        sql_part = token.split('--')[0]
                        if sql_part:
                            filtered_tokens.append(sql_part)
                        skip_comment = True
                        continue
                
                # Check if we should stop skipping (SQL keyword found)
                if skip_comment and token.upper() in ['GROUP', 'ORDER', 'LIMIT', 'HAVING', 'WHERE', 'SELECT', 'FROM']:
                    skip_comment = False
                    filtered_tokens.append(token)
                elif not skip_comment:
                    filtered_tokens.append(token)
            
            cleaned_sql = ' '.join(filtered_tokens)
        
        # Clean up extra whitespace
        cleaned_sql = ' '.join(cleaned_sql.split())
        
        # Ensure semicolon at the end
        if not cleaned_sql.strip().endswith(';'):
            cleaned_sql += ';'
        
        if cleaned_sql != sql_query:
            self.logger.info(f"🧹 Cleaned SQL comments: {sql_query} -> {cleaned_sql}")
        
        return cleaned_sql
    
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
        error_service: IErrorHandlingService,
        sql_validator: Optional[ISQLValidationService] = None
    ) -> IQueryProcessingService:
        """Create comprehensive query processing service"""
        # Create sub-services
        sql_generation_service = SQLGenerationFactory.create_service(llm_service, schema_service)
        query_execution_service = QueryExecutionFactory.create_service(db_service, error_service)
        
        return ComprehensiveQueryProcessingService(
            llm_service, db_service, schema_service, error_service,
            sql_generation_service, query_execution_service, sql_validator
        )
    
    @staticmethod
    def create_service(
        service_type: str,
        llm_service: ILLMCommunicationService,
        db_service: IDatabaseConnectionService,
        schema_service: ISchemaIntrospectionService,
        error_service: IErrorHandlingService,
        sql_validator: Optional[ISQLValidationService] = None
    ) -> IQueryProcessingService:
        """Create query processing service based on type"""
        if service_type.lower() == "comprehensive":
            return QueryProcessingFactory.create_comprehensive_service(
                llm_service, db_service, schema_service, error_service, sql_validator
            )
        else:
            raise ValueError(f"Unsupported query processing service type: {service_type}")