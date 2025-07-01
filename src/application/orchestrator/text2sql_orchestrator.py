"""
Text2SQL Orchestrator - Single Responsibility: Coordinate all services to process user queries
"""
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from ..container.dependency_injection import DependencyContainer, ServiceConfig, ContainerFactory
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


@dataclass
class OrchestratorConfig:
    """Configuration for the orchestrator"""
    max_query_length: int = 1000
    enable_query_history: bool = True
    enable_statistics: bool = True
    session_timeout: int = 3600  # seconds
    enable_conversational_response: bool = True
    conversational_fallback: bool = True
    enable_query_routing: bool = True
    routing_confidence_threshold: float = 0.7


class Text2SQLOrchestrator:
    """
    Main orchestrator that coordinates all services following SRP
    
    Single Responsibility: Coordinate user interaction flow and service communication
    """
    
    def __init__(
        self, 
        container: Optional[DependencyContainer] = None,
        config: Optional[OrchestratorConfig] = None
    ):
        """
        Initialize Text2SQL orchestrator
        
        Args:
            container: Dependency injection container
            config: Orchestrator configuration
        """
        self._container = container or ContainerFactory.create_default_container()
        self._config = config or OrchestratorConfig()
        
        # Initialize container if not already done
        if not self._container._initialized:
            self._container.initialize()
        
        # Get all required services
        self._db_service = self._container.get_service(IDatabaseConnectionService)
        self._llm_service = self._container.get_service(ILLMCommunicationService)
        self._schema_service = self._container.get_service(ISchemaIntrospectionService)
        self._ui_service = self._container.get_service(IUserInterfaceService)
        self._error_service = self._container.get_service(IErrorHandlingService)
        self._query_service = self._container.get_service(IQueryProcessingService)
        
        # Initialize query classification service if enabled
        self._classification_service = None
        if self._config.enable_query_routing:
            try:
                self._classification_service = self._container.get_service(IQueryClassificationService)
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
        
        # Session management
        self._session_id = self._generate_session_id()
        self._query_count = 0
        self._session_start_time = datetime.now()
        
        # Validate all services are working
        self._validate_services()
    
    @property
    def container(self) -> DependencyContainer:
        """Get the dependency container"""
        return self._container
    
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
            health_check = self._container.health_check()
            
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
            self._container.shutdown()
            
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
            "container_health": self._container.health_check()
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