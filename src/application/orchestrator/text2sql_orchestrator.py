"""
Text2SQL Orchestrator - Single Responsibility: Coordinate all services to process user queries
"""
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from ..config.simple_config import ApplicationConfig, OrchestratorConfig
from ..services.database_connection_service import DatabaseConnectionFactory
from ..services.llm_communication_service import LLMCommunicationFactory, LLMConfig
from ..services.schema_introspection_service import SchemaIntrospectionFactory
from ..services.user_interface_service import UserInterfaceFactory
from ..services.error_handling_service import ErrorHandlingFactory
from ..services.query_processing_service import QueryProcessingFactory
from ..services.query_classification_service import QueryClassificationFactory
from ..services.sql_validation_service import SQLValidationFactory
from ...domain.repositories.cid_repository import ICIDRepository
from ...domain.services.cid_semantic_search_service import ICIDSemanticSearchService, CIDSemanticSearchService
from ...infrastructure.repositories.sqlite_cid_repository import SQLiteCIDRepository
from ..services.database_connection_service import IDatabaseConnectionService
from ..services.llm_communication_service import ILLMCommunicationService
from ..services.schema_introspection_service import ISchemaIntrospectionService
from ..services.user_interface_service import (
    IUserInterfaceService, 
    InterfaceType,
    FormattedResponse,
    InputValidator
)
from ..services.error_handling_service import IErrorHandlingService, ErrorCategory
from ..services.query_processing_service import (
    IQueryProcessingService, 
    QueryRequest,
    QueryResult
)
from ..services.conversational_response_service import (
    ConversationalResponseService,
    ConversationContext
)
from ..services.query_classification_service import (
    IQueryClassificationService,
    QueryType,
    QueryClassification
)
# Simple Query Decomposition System
from ..services.simple_query_decomposer import SimpleQueryDecomposer, DecompositionConfig




class Text2SQLOrchestrator:
    """
    Main orchestrator that coordinates all services following SRP
    
    Single Responsibility: Coordinate user interaction flow and service communication
    """
    
    def __init__(
        self, 
        app_config: Optional[ApplicationConfig] = None,
        orchestrator_config: Optional[OrchestratorConfig] = None
    ):
        """
        Initialize Text2SQL orchestrator
        
        Args:
            app_config: Application configuration
            orchestrator_config: Orchestrator configuration
        """
        self._app_config = app_config or ApplicationConfig()
        self._config = orchestrator_config or OrchestratorConfig()
        
        # Initialize services dict first
        self._services = {}
        
        # Initialize all services directly
        self._initialize_services()
        
        # Store services for compatibility after initialization
        self._services.update({
            IDatabaseConnectionService: self._db_service,
            ILLMCommunicationService: self._llm_service,
            ISchemaIntrospectionService: self._schema_service,
            IUserInterfaceService: self._ui_service,
            IErrorHandlingService: self._error_service,
            IQueryProcessingService: self._query_service
        })
        
        # Initialize query classification service if enabled
        self._classification_service = None
        if self._config.enable_query_routing:
            try:
                self._classification_service = QueryClassificationFactory.create_service(
                    llm_service=self._llm_service,
                    error_service=self._error_service,
                    confidence_threshold=self._app_config.query_classification_confidence_threshold
                )
                self._services[IQueryClassificationService] = self._classification_service
            except Exception as e:
                print(f"Warning: Query classification service unavailable: {e}")
                self._config.enable_query_routing = False
        
        # Initialize conversational response service if enabled
        self._conversational_service = None
        if self._config.enable_conversational_response:
            try:
                self._conversational_service = ConversationalResponseService()
            except Exception as e:
                if not self._config.conversational_fallback:
                    raise
                # Log warning but continue without conversational service
                print(f"Warning: Conversational service unavailable: {e}")
        
        # Simple Query Decomposition System
        self._simple_decomposer = None
        if self._config.enable_query_decomposition:
            try:
                decomp_config = DecompositionConfig(
                    enabled=True,
                    complexity_threshold=self._config.decomposition_complexity_threshold,
                    timeout_seconds=self._config.decomposition_timeout_seconds,
                    debug_mode=self._config.decomposition_debug_mode,
                    fallback_enabled=self._config.decomposition_fallback_enabled
                )
                self._simple_decomposer = SimpleQueryDecomposer(
                    query_service=self._query_service,
                    config=decomp_config
                )
                if self._config.decomposition_debug_mode:
                    print("🧩 Simple Query Decomposition System initialized successfully")
            except Exception as e:
                if not self._config.decomposition_fallback_enabled:
                    raise
                print(f"Warning: Simple decomposition system unavailable: {e}")
                self._config.enable_query_decomposition = False
        
        # Session management
        self._session_id = self._generate_session_id()
        self._query_count = 0
        self._session_start_time = datetime.now()
        self._decomposition_stats = {
            "total_decomposed": 0,
            "successful_decompositions": 0,
            "fallback_count": 0,
            "total_time_saved": 0.0
        }
        
        # Validate all services are working
        self._validate_services()
    
    def _initialize_services(self) -> None:
        """Initialize all services directly without DI container"""
        # Initialize database service
        self._db_service = DatabaseConnectionFactory.create_service(
            self._app_config.database_type,
            db_path=self._app_config.database_path
        )
        
        # Initialize error handling service
        self._error_service = ErrorHandlingFactory.create_service(
            self._app_config.error_handling_type,
            enable_logging=self._app_config.enable_error_logging
        )
        
        # Initialize LLM service
        llm_config = LLMConfig(
            model_name=self._app_config.llm_model,
            temperature=self._app_config.llm_temperature,
            timeout=self._app_config.llm_timeout,
            max_retries=self._app_config.llm_max_retries
        )
        self._llm_service = LLMCommunicationFactory.create_service(
            self._app_config.llm_provider,
            model_name=self._app_config.llm_model,
            temperature=self._app_config.llm_temperature,
            timeout=self._app_config.llm_timeout,
            max_retries=self._app_config.llm_max_retries,
            device=getattr(self._app_config, 'llm_device', 'auto'),
            load_in_8bit=getattr(self._app_config, 'llm_load_in_8bit', False),
            load_in_4bit=getattr(self._app_config, 'llm_load_in_4bit', True)
        )
        
        # Initialize schema service
        self._schema_service = SchemaIntrospectionFactory.create_service(
            self._app_config.schema_type,
            self._db_service
        )
        
        # Initialize SQL validation service
        self._sql_validator = SQLValidationFactory.create_comprehensive_validator()
        
        # Initialize query processing service
        self._query_service = QueryProcessingFactory.create_service(
            self._app_config.query_processing_type,
            self._llm_service,
            self._db_service,
            self._schema_service,
            self._error_service,
            self._sql_validator
        )
        
        # Initialize UI service
        self._ui_service = UserInterfaceFactory.create_service(
            self._app_config.ui_type,
            interface_type=self._app_config.interface_type
        )
        
        # Initialize CID repository if enabled
        if self._app_config.enable_cid_semantic_search:
            if self._app_config.cid_repository_type == "sqlite":
                self._cid_repository = SQLiteCIDRepository(self._app_config.database_path)
                self._cid_semantic_service = CIDSemanticSearchService(self._cid_repository)
                self._services[ICIDRepository] = self._cid_repository
                self._services[ICIDSemanticSearchService] = self._cid_semantic_service
    
    def get_service(self, service_type):
        """Get service instance (compatibility with old container interface)"""
        return self._services.get(service_type)
    
    def get_schema_introspection_service(self):
        """Get schema introspection service"""
        return self._schema_service
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on all services"""
        health_status = {
            "status": "healthy",
            "services": {},
            "timestamp": None
        }
        
        try:
            # Check database service
            health_status["services"]["database"] = {
                "healthy": self._db_service.test_connection(),
                "type": self._app_config.database_type
            }
            
            # Check LLM service
            health_status["services"]["llm"] = {
                "healthy": self._llm_service.is_available(),
                "model_info": self._llm_service.get_model_info()
            }
            
            # Check other services existence
            health_status["services"]["schema"] = {
                "healthy": self._schema_service is not None
            }
            health_status["services"]["query_processing"] = {
                "healthy": self._query_service is not None
            }
            health_status["services"]["ui"] = {
                "healthy": self._ui_service is not None
            }
            health_status["services"]["error_handling"] = {
                "healthy": self._error_service is not None
            }
            
            # Check query classification service if enabled
            if self._config.enable_query_routing:
                health_status["services"]["query_classification"] = {
                    "healthy": self._classification_service is not None
                }
            
            # Check CID services if enabled
            if self._app_config.enable_cid_semantic_search:
                health_status["services"]["cid_repository"] = {
                    "healthy": hasattr(self, '_cid_repository') and self._cid_repository is not None
                }
                health_status["services"]["cid_semantic_search"] = {
                    "healthy": hasattr(self, '_cid_semantic_service') and self._cid_semantic_service is not None
                }
            
            # Determine overall health
            all_healthy = all(
                service_health.get("healthy", False) 
                for service_health in health_status["services"].values()
            )
            health_status["status"] = "healthy" if all_healthy else "degraded"
            
        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["error"] = str(e)
        
        from datetime import datetime
        health_status["timestamp"] = datetime.now().isoformat()
        
        return health_status
    
    def shutdown(self) -> None:
        """Shutdown all services gracefully"""
        try:
            # Close database connections
            if self._db_service:
                self._db_service.close_connection()
            
            # Clear all services
            self._services.clear()
            
        except Exception as e:
            # Log error but don't raise - we're shutting down anyway
            print(f"Warning: Error during shutdown: {str(e)}")
    
    def start_interactive_session(self) -> None:
        """Start interactive user session"""
        try:
            self._display_system_status()
            
            while True:
                try:
                    # Get user input
                    user_input = self._ui_service.get_user_input("Sua pergunta:")
                    
                    if not user_input:
                        continue
                    
                    # Handle special commands
                    if self._handle_special_commands(user_input):
                        continue
                    
                    # Check for exit commands
                    if user_input.lower() in ['sair', 'quit', 'exit', 'q']:
                        self._display_goodbye()
                        break
                    
                    # Process the query
                    self._process_user_query(user_input)
                    
                except KeyboardInterrupt:
                    print("\n\nSessão interrompida pelo usuário.")
                    break
                except Exception as e:
                    error_info = self._error_service.handle_error(e, ErrorCategory.SYSTEM)
                    error_message = self._error_service.get_user_friendly_message(error_info)
                    self._ui_service.display_error(error_message)
        
        finally:
            self._cleanup_session()
    
    def process_single_query(self, query: str) -> QueryResult:
        """
        Process a single query without interactive session
        
        Args:
            query: User query string
            
        Returns:
            Query result
        """
        try:
            # Validate input
            if not InputValidator.validate_query_length(query, self._config.max_query_length):
                raise ValueError(f"Query too long (max {self._config.max_query_length} characters)")
            
            # Sanitize input
            sanitized_query = InputValidator.sanitize_input(query)
            
            # Step 1: Classify query if routing is enabled
            classification = None
            if self._config.enable_query_routing and self._classification_service:
                try:
                    classification = self._classification_service.classify_query(sanitized_query)
                    print(f"🔍 Query classificada como: {classification.query_type.value} (confiança: {classification.confidence_score:.2f})")
                except Exception as e:
                    print(f"Warning: Classification failed, falling back to SQL processing: {e}")
            
            # Step 2: Route based on classification
            if (classification and 
                classification.query_type == QueryType.CONVERSATIONAL_QUERY and 
                classification.confidence_score >= self._config.routing_confidence_threshold):
                
                # Route to conversational service directly
                result = self._process_conversational_query(sanitized_query, classification)
            else:
                # Simple decomposition check - placeholder for new system
                if (self._config.enable_query_decomposition and 
                    self._simple_decomposer):
                    
                    # Use simple decomposition (to be implemented)
                    result = self._process_with_simple_decomposition(sanitized_query)
                else:
                    # Route to SQL processing (default behavior)
                    request = QueryRequest(
                        user_query=sanitized_query,
                        session_id=self._session_id,
                        timestamp=datetime.now()
                    )
                    result = self._query_service.process_natural_language_query(request)
            
            self._query_count += 1
            
            # Add classification metadata if available
            if classification and hasattr(result, 'metadata'):
                if result.metadata is None:
                    result.metadata = {}
                result.metadata.update({
                    "query_classification": classification.query_type.value,
                    "classification_confidence": classification.confidence_score,
                    "routing_applied": True,
                    "classification_reasoning": classification.reasoning
                })
            
            return result
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.QUERY_PROCESSING)
            return QueryResult(
                sql_query="",
                results=[],
                success=False,
                execution_time=0.0,
                row_count=0,
                error_message=error_info.message
            )
    
    def _process_user_query(self, user_input: str) -> None:
        """Process user query and display results"""
        try:
            # Process the query
            result = self.process_single_query(user_input)
            
            # Format and display response
            formatted_response = self._format_query_result(result, user_input)
            self._ui_service.display_response(formatted_response)
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.QUERY_PROCESSING)
            error_message = self._error_service.get_user_friendly_message(error_info)
            self._ui_service.display_error(error_message)
    
    def _handle_special_commands(self, user_input: str) -> bool:
        """
        Handle special commands
        
        Returns:
            True if command was handled, False otherwise
        """
        command = user_input.lower().strip()
        
        if command in ['schema', 'esquema']:
            self._display_schema_info()
            return True
        
        elif command in ['exemplos', 'examples']:
            self._display_examples()
            return True
        
        elif command in ['ajuda', 'help']:
            self._ui_service.display_help()
            return True
        
        elif command in ['status', 'estado']:
            self._display_system_status()
            return True
        
        elif command in ['estatisticas', 'stats']:
            self._display_statistics()
            return True
        
        elif command in ['historico', 'history']:
            self._display_query_history()
            return True
        
        return False
    
    def _display_schema_info(self) -> None:
        """Display database schema information"""
        try:
            schema_context = self._schema_service.get_schema_context()
            
            response = FormattedResponse(
                content=f"📊 Informações do Schema:\n\n{schema_context.formatted_context}",
                success=True
            )
            self._ui_service.display_response(response)
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.DATABASE)
            error_message = self._error_service.get_user_friendly_message(error_info)
            self._ui_service.display_error(error_message)
    
    def _display_examples(self) -> None:
        """Display query examples"""
        examples = [
            "Quantos pacientes existem no banco?",
            "Qual a idade média dos pacientes?",
            "Quantas mortes ocorreram em Porto Alegre?",
            "Quais são os 5 diagnósticos mais comuns?",
            "Qual o custo total por estado?",
            "Quantos pacientes são do sexo masculino?",
            "Qual a média de dias de UTI por paciente?"
        ]
        
        examples_text = "\n".join([f"• {example}" for example in examples])
        
        response = FormattedResponse(
            content=f"💡 Exemplos de perguntas:\n\n{examples_text}",
            success=True
        )
        self._ui_service.display_response(response)
    
    def _display_system_status(self) -> None:
        """Display system status"""
        try:
            health_check = self.health_check()
            
            status_text = f"🔍 Status do Sistema: {health_check['status'].upper()}\n\n"
            
            for service_name, service_health in health_check['services'].items():
                status_icon = "✅" if service_health.get('healthy', False) else "❌"
                status_text += f"{status_icon} {service_name.title()}: {'OK' if service_health.get('healthy', False) else 'ERRO'}\n"
            
            response = FormattedResponse(
                content=status_text,
                success=health_check['status'] in ['healthy', 'degraded']
            )
            self._ui_service.display_response(response)
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.SYSTEM)
            error_message = self._error_service.get_user_friendly_message(error_info)
            self._ui_service.display_error(error_message)
    
    def _display_statistics(self) -> None:
        """Display session statistics"""
        if not self._config.enable_statistics:
            self._ui_service.display_error("Estatísticas não estão habilitadas")
            return
        
        try:
            query_stats = self._query_service.get_query_statistics()
            error_stats = self._error_service.get_error_statistics()
            
            session_duration = (datetime.now() - self._session_start_time).total_seconds()
            
            stats_text = f"""📈 Estatísticas da Sessão:

            🕐 Duração: {session_duration:.1f} segundos
            🔢 Consultas processadas: {self._query_count}
            ✅ Taxa de sucesso: {query_stats.get('success_rate', 0):.1f}%
            ⏱️ Tempo médio de execução: {query_stats.get('average_execution_time', 0):.2f}s
            ❌ Total de erros: {error_stats.get('total_errors', 0)}
            🆔 ID da sessão: {self._session_id}
            """
            
            response = FormattedResponse(
                content=stats_text,
                success=True
            )
            self._ui_service.display_response(response)
            
        except Exception as e:
            error_info = self._error_service.handle_error(e, ErrorCategory.SYSTEM)
            error_message = self._error_service.get_user_friendly_message(error_info)
            self._ui_service.display_error(error_message)
    
    def _display_query_history(self) -> None:
        """Display query history (simplified)"""
        response = FormattedResponse(
            content="📚 Histórico de consultas disponível apenas via API de estatísticas",
            success=True
        )
        self._ui_service.display_response(response)
    
    def _format_query_result(self, result: QueryResult, user_query: str = "") -> FormattedResponse:
        """Format query result for display with optional conversational response"""
        
        # Try to generate conversational response if enabled and available
        if (self._config.enable_conversational_response and 
            self._conversational_service and 
            self._conversational_service.is_conversational_llm_available()):
            
            try:
                conversational_response = self._conversational_service.generate_response(
                    user_query=user_query,
                    sql_query=result.sql_query,
                    sql_results=result.results,
                    session_id=self._session_id,
                    error_message=result.error_message or ""
                )
                
                # Use conversational response with enhanced metadata
                return FormattedResponse(
                    content=conversational_response.message,
                    success=result.success,
                    execution_time=result.execution_time,
                    metadata={
                        "sql_query": result.sql_query,
                        "row_count": result.row_count,
                        "conversational_response": True,
                        "response_type": conversational_response.response_type.value,
                        "confidence_score": conversational_response.confidence_score,
                        "suggestions": conversational_response.suggestions,
                        "processing_time": conversational_response.processing_time,
                        # Include routing information if available
                        **(result.metadata or {})
                    }
                )
                
            except Exception as e:
                # Fallback to basic formatting if conversational response fails
                if not self._config.conversational_fallback:
                    raise
                print(f"Warning: Conversational response failed, using basic format: {e}")
        
        # Basic formatting (original logic)
        if result.success:
            # Add sample results if available
            if result.results:
                if len(result.results) == 1 and len(result.results[0]) == 1:
                    # Single value result
                    value = list(result.results[0].values())[0]
                    content = f"Resultado: {value}"
                else:
                    # Multiple results - show summary
                    content = f"Encontrados {result.row_count} registros"
                    if result.row_count <= 5:
                        content += f"\n\nResultados:\n"
                        for i, row in enumerate(result.results[:5], 1):
                            content += f"{i}. {row}\n"
            else:
                content = f"Resultado: {result.row_count} registros encontrados"
            
            return FormattedResponse(
                content=content,
                success=True,
                execution_time=result.execution_time,
                metadata={
                    "sql_query": result.sql_query,
                    "row_count": result.row_count,
                    "conversational_response": False,
                    # Include routing information if available
                    **(result.metadata or {})
                }
            )
        else:
            return FormattedResponse(
                content=result.error_message or "Erro desconhecido",
                success=False,
                execution_time=result.execution_time,
                metadata={
                    "conversational_response": False,
                    # Include routing information if available
                    **(result.metadata or {})
                }
            )
    
    def _validate_services(self) -> None:
        """Validate that all required services are available"""
        required_services = [
            self._db_service,
            self._llm_service,
            self._schema_service,
            self._ui_service,
            self._error_service,
            self._query_service
        ]
        
        for service in required_services:
            if service is None:
                raise RuntimeError("Required service not available")
    
    def _generate_session_id(self) -> str:
        """Generate unique session ID"""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def _display_goodbye(self) -> None:
        """Display goodbye message"""
        goodbye_text = f"""
        🎉 Obrigado por usar o TXT2SQL Claude!
        
        📊 Resumo da sessão:
           • Consultas processadas: {self._query_count}
           • ID da sessão: {self._session_id}
           • Duração: {(datetime.now() - self._session_start_time).total_seconds():.1f}s
        
        Até a próxima! 👋
        """
        
        response = FormattedResponse(content=goodbye_text, success=True)
        self._ui_service.display_response(response)
    
    def _cleanup_session(self) -> None:
        """Clean up session resources"""
        try:
            # Close database connections
            self._db_service.close_connection()
            
            # Shutdown container
            self.shutdown()
            
        except Exception as e:
            # Log but don't raise during cleanup
            print(f"Warning: Error during cleanup: {str(e)}")
    
    def _process_conversational_query(self, query: str, classification: QueryClassification) -> QueryResult:
        """
        Process conversational query directly without SQL execution
        
        Args:
            query: User's natural language question
            classification: Query classification result
            
        Returns:
            QueryResult with conversational response
        """
        import time
        start_time = time.time()
        
        try:
            print(f"💬 Processando query conversacional: {query[:50]}...")
            
            # Use conversational service to generate direct response
            if self._conversational_service:
                # Generate conversational response without SQL execution
                conversational_response = self._conversational_service.generate_response(
                    user_query=query,
                    sql_query="",  # No SQL for direct conversational queries
                    sql_results=[],  # No SQL results
                    session_id=self._session_id,
                    context={"direct_conversational": True, "classification": classification.reasoning}
                )
                
                execution_time = time.time() - start_time
                
                return QueryResult(
                    sql_query="[CONVERSATIONAL_QUERY]",
                    results=[{"conversational_response": conversational_response.message}],
                    success=True,
                    execution_time=execution_time,
                    row_count=1,
                    metadata={
                        "query_type": "conversational",
                        "routing_method": "direct_conversational",
                        "classification_confidence": classification.confidence_score,
                        "response_type": conversational_response.response_type.value,
                        "suggestions": conversational_response.suggestions
                    }
                )
            else:
                # Fallback: generate basic conversational response
                execution_time = time.time() - start_time
                
                basic_response = self._generate_basic_conversational_response(query, classification)
                
                return QueryResult(
                    sql_query="[CONVERSATIONAL_QUERY]",
                    results=[{"conversational_response": basic_response}],
                    success=True,
                    execution_time=execution_time,
                    row_count=1,
                    metadata={
                        "query_type": "conversational",
                        "routing_method": "basic_fallback",
                        "classification_confidence": classification.confidence_score
                    }
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            error_info = self._error_service.handle_error(e, ErrorCategory.QUERY_PROCESSING)
            
            return QueryResult(
                sql_query="",
                results=[],
                success=False,
                execution_time=execution_time,
                row_count=0,
                error_message=f"Erro na resposta conversacional: {error_info.message}",
                metadata={
                    "query_type": "conversational",
                    "routing_method": "error_fallback"
                }
            )
    
    def _generate_basic_conversational_response(self, query: str, classification: QueryClassification) -> str:
        """Generate basic conversational response when conversational service is unavailable"""
        
        # Try to use LLM directly for basic conversational response
        try:
            if self._llm_service:
                basic_prompt = f"""
Você é um assistente especializado em saúde e sistema SUS brasileiro.

PERGUNTA DO USUÁRIO: {query}

CLASSIFICAÇÃO: {classification.reasoning}

Forneça uma resposta clara, informativa e direta para a pergunta. 
Se for sobre códigos CID, explique o significado.
Se for sobre conceitos médicos, forneça definições simples.
Se for sobre o SUS, explique de forma didática.

Mantenha a resposta concisa mas completa.

RESPOSTA:"""
                
                llm_response = self._llm_service.send_prompt(basic_prompt)
                return llm_response.content
            
        except Exception as e:
            print(f"Warning: LLM fallback failed: {e}")
        
        # Ultimate fallback: template-based response
        if "cid" in query.lower() or "código" in query.lower():
            return f"""
Esta é uma pergunta sobre classificação médica (CID-10).

**Sua pergunta:** {query}

**Explicação:** O sistema CID-10 (Classificação Internacional de Doenças) é usado para codificar diagnósticos médicos. Cada código representa uma condição específica.

Para obter informações mais detalhadas sobre códigos específicos, recomendo consultar fontes médicas oficiais ou profissionais de saúde.
"""
        else:
            return f"""
**Sua pergunta:** {query}

Esta é uma pergunta conceitual sobre saúde ou o sistema SUS. 

Para fornecer uma resposta mais precisa e detalhada, seria necessário acesso ao serviço conversacional completo. 

**Sugestão:** Reformule sua pergunta como uma consulta específica aos dados (ex: "Quantos casos de...", "Qual a média de...") para obter informações baseadas nos dados disponíveis.
"""
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get current session information"""
        return {
            "session_id": self._session_id,
            "start_time": self._session_start_time.isoformat(),
            "query_count": self._query_count,
            "duration_seconds": (datetime.now() - self._session_start_time).total_seconds(),
            "container_health": self.health_check()
        }
    
    def process_conversational_query(self, query: str) -> Dict[str, Any]:
        """
        Process query and return conversational response instead of raw SQL results
        
        Args:
            query: User's natural language question
            
        Returns:
            Dictionary with conversational response and metadata
        """
        # Process the query normally
        result = self.process_single_query(query)
        
        # Format with conversational response
        formatted_response = self._format_query_result(result, query)
        
        return {
            "success": result.success,
            "question": query,
            "response": formatted_response.content,
            "execution_time": formatted_response.execution_time,
            "error_message": result.error_message if not result.success else None,
            "metadata": formatted_response.metadata,
            "timestamp": datetime.now().isoformat()
        }
    
    # Simple Query Decomposition Methods
    
    def _process_with_simple_decomposition(self, query: str) -> QueryResult:
        """
        Process query with simple decomposition system
        
        Args:
            query: Sanitized user query
            
        Returns:
            QueryResult from decomposition or fallback
        """
        if not self._simple_decomposer:
            return self._fallback_to_standard_processing(query)
        
        try:
            result = self._simple_decomposer.decompose_and_execute(query)
            
            # Update session stats if decomposition was used
            if (result.metadata and 
                result.metadata.get("decomposition_used", False)):
                self._decomposition_stats["total_decomposed"] += 1
                if result.success:
                    self._decomposition_stats["successful_decompositions"] += 1
            
            return result
            
        except Exception as e:
            if self._config.decomposition_debug_mode:
                print(f"❌ Simple decomposition error: {e}")
            self._decomposition_stats["fallback_count"] += 1
            return self._fallback_to_standard_processing(query)
    
    # Complex decomposition methods - temporarily disabled during refactoring
    def _process_with_decomposition_check_DISABLED(self, query: str) -> QueryResult:
        """
        Process query with decomposition check and fallback
        
        Args:
            query: Sanitized user query
            
        Returns:
            QueryResult from either decomposition or standard processing
        """
        decomposition_start_time = datetime.now()
        
        try:
            # Check if query should be decomposed
            if self._config.decomposition_debug_mode:
                print(f"🔍 Checking if query should be decomposed...")
            
            should_decompose = self._query_planner.should_decompose_query(query)
            
            if should_decompose:
                if self._config.show_decomposition_progress:
                    print(f"🧩 Query complexa detectada - iniciando decomposição...")
                
                # Update stats
                self._decomposition_stats["total_decomposed"] += 1
                
                # Execute decomposition
                result = self._execute_decomposed_query(query, decomposition_start_time)
                
                if result.success:
                    self._decomposition_stats["successful_decompositions"] += 1
                    if self._config.show_decomposition_progress:
                        print(f"✅ Decomposição executada com sucesso!")
                    return result
                else:
                    # Decomposition failed, try fallback
                    if self._config.decomposition_fallback_enabled:
                        if self._config.show_decomposition_progress:
                            print(f"⚠️ Decomposição falhou, usando processamento padrão...")
                        return self._fallback_to_standard_processing(query)
                    else:
                        return result
            else:
                # Query doesn't need decomposition
                if self._config.decomposition_debug_mode:
                    print(f"📝 Query não atende critérios para decomposição")
                return self._fallback_to_standard_processing(query)
                
        except Exception as e:
            # Error in decomposition system
            if self._config.decomposition_fallback_enabled:
                if self._config.show_decomposition_progress:
                    print(f"❌ Erro no sistema de decomposição: {e}")
                    print(f"🔄 Usando processamento padrão...")
                self._decomposition_stats["fallback_count"] += 1
                return self._fallback_to_standard_processing(query)
            else:
                raise
    
    def _execute_decomposed_query_DISABLED(self, query: str, start_time: datetime) -> QueryResult:
        """
        Execute query using decomposition system
        
        Args:
            query: User query
            start_time: Start time for performance measurement
            
        Returns:
            QueryResult from decomposition execution
        """
        try:
            # Generate execution plan
            plan = self._query_planner.create_execution_plan(query)
            
            if self._config.show_decomposition_progress:
                print(f"📋 Plano gerado: {len(plan.steps)} etapas usando estratégia {plan.strategy.value}")
                
                if self._config.decomposition_debug_mode:
                    for i, step in enumerate(plan.steps, 1):
                        print(f"   {i}. {step.description}")
            
            # Execute plan with progress callback if enabled
            if self._config.show_decomposition_progress:
                def progress_callback(progress):
                    print(f"   📊 Progresso: {progress.overall_progress:.0%} - {progress.current_step_description}")
                
                execution_result = self._execution_orchestrator.execute_plan_async(plan, progress_callback)
            else:
                execution_result = self._execution_orchestrator.execute_plan(plan)
            
            # Convert PlanExecutionResult to QueryResult
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            if execution_result.success:
                # Calculate time potentially saved
                estimated_standard_time = len(plan.steps) * 10.0  # Estimate
                time_saved = max(0, estimated_standard_time - total_execution_time)
                self._decomposition_stats["total_time_saved"] += time_saved
                
                # Create successful QueryResult
                return QueryResult(
                    sql_query=f"Decomposed query with {len(plan.steps)} steps",
                    results=execution_result.final_results,
                    success=True,
                    execution_time=total_execution_time,
                    row_count=execution_result.final_row_count,
                    metadata={
                        "decomposition_used": True,
                        "plan_id": plan.plan_id,
                        "strategy": plan.strategy.value,
                        "steps_executed": len(execution_result.completed_steps),
                        "total_steps": len(plan.steps),
                        "estimated_time_saved": time_saved,
                        "complexity_score": plan.complexity_score,
                        "execution_metadata": execution_result.metadata,
                        "generator": execution_result.metadata.get("formatted_result", {}).get("generator", "unknown") if execution_result.metadata else "unknown"
                    }
                )
            else:
                # Decomposition execution failed
                return QueryResult(
                    sql_query="",
                    results=[],
                    success=False,
                    execution_time=total_execution_time,
                    row_count=0,
                    error_message=f"Decomposition execution failed: {execution_result.error_message}",
                    metadata={
                        "decomposition_used": True,
                        "decomposition_failed": True,
                        "plan_id": plan.plan_id,
                        "failed_step": execution_result.failed_step_id
                    }
                )
                
        except Exception as e:
            total_execution_time = (datetime.now() - start_time).total_seconds()
            return QueryResult(
                sql_query="",
                results=[],
                success=False,
                execution_time=total_execution_time,
                row_count=0,
                error_message=f"Decomposition system error: {str(e)}",
                metadata={
                    "decomposition_used": True,
                    "decomposition_error": True
                }
            )
    
    def _fallback_to_standard_processing(self, query: str) -> QueryResult:
        """
        Fallback to standard query processing
        
        Args:
            query: User query
            
        Returns:
            QueryResult from standard processing
        """
        request = QueryRequest(
            user_query=query,
            session_id=self._session_id,
            timestamp=datetime.now()
        )
        
        result = self._query_service.process_natural_language_query(request)
        
        # Add metadata to indicate standard processing was used
        if hasattr(result, 'metadata'):
            if result.metadata is None:
                result.metadata = {}
            result.metadata.update({
                "decomposition_used": False,
                "processing_method": "standard"
            })
        
        return result
    
    def get_decomposition_statistics(self) -> Dict[str, Any]:
        """
        Get decomposition system statistics
        
        Returns:
            Dictionary with decomposition statistics
        """
        total_queries = self._query_count
        decomposed_queries = self._decomposition_stats["total_decomposed"]
        
        base_stats = {
            "decomposition_enabled": self._config.enable_query_decomposition,
            "total_queries_processed": total_queries,
            "queries_decomposed": decomposed_queries,
            "decomposition_rate": (decomposed_queries / total_queries * 100) if total_queries > 0 else 0,
            "successful_decompositions": self._decomposition_stats["successful_decompositions"],
            "success_rate": (self._decomposition_stats["successful_decompositions"] / decomposed_queries * 100) if decomposed_queries > 0 else 0,
            "fallback_count": self._decomposition_stats["fallback_count"],
            "total_time_saved_seconds": self._decomposition_stats["total_time_saved"],
            "configuration": {
                "complexity_threshold": self._config.decomposition_complexity_threshold,
                "timeout_seconds": self._config.decomposition_timeout_seconds,
                "fallback_enabled": self._config.decomposition_fallback_enabled,
                "debug_mode": self._config.decomposition_debug_mode
            }
        }
        
        # Add simple decomposer statistics if available
        if self._simple_decomposer:
            simple_stats = self._simple_decomposer.get_statistics()
            base_stats["simple_decomposer"] = simple_stats
        
        return base_stats
    
    def set_decomposition_debug_mode(self, enabled: bool) -> None:
        """
        Enable or disable decomposition debug mode
        
        Args:
            enabled: True to enable debug mode, False to disable
        """
        self._config.decomposition_debug_mode = enabled
        if enabled:
            print("🐛 Decomposition debug mode enabled")
        else:
            print("🔇 Decomposition debug mode disabled")
    
    def get_enhanced_statistics(self) -> Dict[str, Any]:
        """
        Get enhanced orchestrator statistics including decomposition and performance
        
        Returns:
            Complete statistics including decomposition and performance data
        """
        base_stats = self.get_session_info()
        decomposition_stats = self.get_decomposition_statistics()
        
        # Performance statistics - disabled during refactoring
        performance_stats = {"disabled": "Performance optimization temporarily disabled"}
        
        return {
            **base_stats,
            "decomposition_statistics": decomposition_stats,
            "performance_statistics": performance_stats,  # NEW
            "services_status": {
                "query_classification": self._classification_service is not None,
                "conversational_response": self._conversational_service is not None,
                "query_decomposition": (self._simple_decomposer is not None),
                "performance_optimization": False  # Disabled during refactoring
            },
            "configuration": {
                "enable_query_routing": self._config.enable_query_routing,
                "enable_conversational_response": self._config.enable_conversational_response,
                "enable_query_decomposition": self._config.enable_query_decomposition,
                "routing_confidence_threshold": self._config.routing_confidence_threshold,
                "decomposition_complexity_threshold": self._config.decomposition_complexity_threshold
            }
        }
    
    # NEW: Performance monitoring methods (Checkpoint 9)
    
    def get_performance_statistics(self) -> Dict[str, Any]:
        """
        Get detailed performance statistics for cache and parallel execution
        
        Returns:
            Performance statistics including cache hit rates and parallel efficiency
        """
        if not (self._execution_orchestrator and 
                hasattr(self._execution_orchestrator, 'get_performance_statistics')):
            return {
                "performance_optimization_enabled": False,
                "message": "Performance optimization not available"
            }
        
        return self._execution_orchestrator.get_performance_statistics()
    
    def optimize_system_performance(self) -> Dict[str, Any]:
        """
        Execute system performance optimization (cache cleanup, etc.)
        
        Returns:
            Results of optimization operations
        """
        optimization_results = {
            "timestamp": datetime.now().isoformat(),
            "operations_performed": []
        }
        
        # Optimize decomposition system performance
        if (self._execution_orchestrator and 
            hasattr(self._execution_orchestrator, 'optimize_performance')):
            
            decomp_optimization = self._execution_orchestrator.optimize_performance()
            optimization_results["decomposition_optimization"] = decomp_optimization
            optimization_results["operations_performed"].append("decomposition_cache_optimization")
        
        # Could add other system optimizations here
        optimization_results["total_operations"] = len(optimization_results["operations_performed"])
        
        return optimization_results
    
    def get_cache_performance(self) -> Dict[str, Any]:
        """
        Get cache performance metrics
        
        Returns:
            Cache hit rates and memory usage statistics
        """
        if not (self._execution_orchestrator and 
                hasattr(self._execution_orchestrator, 'get_cache_hit_rate')):
            return {"cache_enabled": False}
        
        return {
            "cache_enabled": True,
            "cache_hit_rate": self._execution_orchestrator.get_cache_hit_rate(),
            "parallel_efficiency": self._execution_orchestrator.get_parallel_efficiency(),
            "optimization_enabled": getattr(self._execution_orchestrator, 'enable_performance_optimization', False)
        }
    
    def enable_performance_debug_mode(self, enabled: bool = True):
        """
        Enable performance debug mode for detailed monitoring
        
        Args:
            enabled: True to enable, False to disable
        """
        if (self._execution_orchestrator and 
            hasattr(self._execution_orchestrator, 'enable_performance_monitoring')):
            self._execution_orchestrator.enable_performance_monitoring(enabled)
            print(f"🔍 Performance debug mode: {'enabled' if enabled else 'disabled'}")
        else:
            print("⚠️ Performance monitoring not available")
    
    def get_system_health_with_performance(self) -> Dict[str, Any]:
        """
        Get comprehensive system health including performance metrics
        
        Returns:
            System health with performance indicators
        """
        base_health = self.health_check()
        
        # Add performance health indicators
        performance_health = {
            "cache_system": "unknown",
            "parallel_execution": "unknown",
            "optimization_status": "unknown"
        }
        
        if (self._execution_orchestrator and 
            hasattr(self._execution_orchestrator, 'get_performance_statistics')):
            
            perf_stats = self._execution_orchestrator.get_performance_statistics()
            
            # Determine cache health
            if perf_stats.get("optimization_enabled"):
                cache_stats = perf_stats.get("cache_statistics", {})
                cache_hit_rate = cache_stats.get("execution_results", {}).get("hit_rate", 0)
                
                if cache_hit_rate > 70:
                    performance_health["cache_system"] = "healthy"
                elif cache_hit_rate > 30:
                    performance_health["cache_system"] = "degraded"
                else:
                    performance_health["cache_system"] = "poor"
                
                # Determine parallel execution health
                parallel_stats = perf_stats.get("parallel_statistics", {})
                if parallel_stats:
                    performance_health["parallel_execution"] = "healthy"
                else:
                    performance_health["parallel_execution"] = "unavailable"
                
                performance_health["optimization_status"] = "enabled"
            else:
                performance_health["optimization_status"] = "disabled"
        
        return {
            **base_health,
            "performance_health": performance_health
        }