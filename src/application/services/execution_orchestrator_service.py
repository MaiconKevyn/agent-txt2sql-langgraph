"""
Execution Orchestrator Service Interface
Interface para orquestração de execução de planos decompostos
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from domain.entities.query_decomposition import (
    QueryPlan,
    QueryStep,
    StepExecutionResult,
    PlanExecutionResult,
    ExecutionContext
)


class ExecutionMode(Enum):
    """Modos de execução disponíveis"""
    SEQUENTIAL = "sequential"          # Execução sequencial (padrão)
    PARALLEL = "parallel"             # Execução paralela quando possível
    FAIL_FAST = "fail_fast"           # Para na primeira falha
    BEST_EFFORT = "best_effort"       # Continua mesmo com falhas parciais


class StepStatus(Enum):
    """Status de execução de uma etapa"""
    PENDING = "pending"               # Aguardando execução
    RUNNING = "running"               # Executando
    COMPLETED = "completed"           # Concluída com sucesso
    FAILED = "failed"                 # Falhou
    SKIPPED = "skipped"               # Pulada (dependência falhou)
    RETRYING = "retrying"             # Tentando novamente


@dataclass
class OrchestratorConfig:
    """Configuração do orquestrador de execução"""
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    max_retries_per_step: int = 2
    retry_delay_seconds: float = 1.0
    step_timeout_seconds: float = 30.0
    total_timeout_seconds: float = 300.0
    enable_step_caching: bool = True
    enable_rollback: bool = True
    preserve_intermediate_results: bool = True
    log_detailed_execution: bool = True


@dataclass
class ExecutionProgress:
    """Progresso da execução de um plano"""
    plan_id: str
    total_steps: int
    completed_steps: int
    failed_steps: int
    current_step_id: Optional[int]
    current_step_description: str
    elapsed_time: float
    estimated_remaining_time: Optional[float]
    overall_progress: float  # 0.0 a 1.0
    
    @property
    def is_complete(self) -> bool:
        """Indica se a execução foi completada"""
        return self.completed_steps + self.failed_steps == self.total_steps


@dataclass
class StepExecutionContext:
    """Contexto de execução para uma etapa específica"""
    step: QueryStep
    plan: QueryPlan
    previous_results: Dict[int, StepExecutionResult]
    execution_variables: Dict[str, Any]
    retry_count: int = 0
    start_time: Optional[float] = None


class IStepExecutor(ABC):
    """
    Interface para execução de etapas individuais
    """
    
    @abstractmethod
    def execute_step(self, context: StepExecutionContext) -> StepExecutionResult:
        """
        Executa uma etapa específica do plano
        
        Args:
            context: Contexto de execução da etapa
            
        Returns:
            Resultado da execução da etapa
        """
        pass
    
    @abstractmethod
    def prepare_step_sql(self, context: StepExecutionContext) -> str:
        """
        Prepara o SQL final para execução, substituindo parâmetros
        
        Args:
            context: Contexto de execução
            
        Returns:
            SQL pronto para execução
        """
        pass
    
    @abstractmethod
    def validate_step_prerequisites(self, context: StepExecutionContext) -> List[str]:
        """
        Valida se os pré-requisitos da etapa foram atendidos
        
        Args:
            context: Contexto de execução
            
        Returns:
            Lista de problemas encontrados (vazia se OK)
        """
        pass


class IResultAggregator(ABC):
    """
    Interface para agregação de resultados de múltiplas etapas
    """
    
    @abstractmethod
    def combine_step_results(
        self, 
        step_results: List[StepExecutionResult],
        plan: QueryPlan
    ) -> List[Dict[str, Any]]:
        """
        Combina resultados de múltiplas etapas em resultado final
        
        Args:
            step_results: Resultados das etapas executadas
            plan: Plano original
            
        Returns:
            Resultado final combinado
        """
        pass
    
    @abstractmethod
    def format_final_result(
        self,
        combined_results: List[Dict[str, Any]],
        original_query: str,
        plan: QueryPlan
    ) -> Dict[str, Any]:
        """
        Formata o resultado final para apresentação
        
        Args:
            combined_results: Resultados combinados
            original_query: Query original do usuário
            plan: Plano executado
            
        Returns:
            Resultado formatado para o usuário
        """
        pass


class IExecutionStateManager(ABC):
    """
    Interface para gerenciamento de estado da execução
    """
    
    @abstractmethod
    def save_execution_state(
        self, 
        plan_id: str, 
        progress: ExecutionProgress,
        step_results: List[StepExecutionResult]
    ) -> bool:
        """
        Salva o estado atual da execução
        
        Args:
            plan_id: ID do plano
            progress: Progresso atual
            step_results: Resultados das etapas
            
        Returns:
            True se salvo com sucesso
        """
        pass
    
    @abstractmethod
    def load_execution_state(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Carrega estado salvo de uma execução
        
        Args:
            plan_id: ID do plano
            
        Returns:
            Estado salvo ou None se não encontrado
        """
        pass
    
    @abstractmethod
    def clear_execution_state(self, plan_id: str) -> bool:
        """
        Limpa estado salvo de uma execução
        
        Args:
            plan_id: ID do plano
            
        Returns:
            True se removido com sucesso
        """
        pass


class IExecutionOrchestrator(ABC):
    """
    Interface principal do orquestrador de execução
    """
    
    @abstractmethod
    def execute_plan(self, plan: QueryPlan) -> PlanExecutionResult:
        """
        Executa um plano de decomposição completo
        
        Args:
            plan: Plano a ser executado
            
        Returns:
            Resultado completo da execução
        """
        pass
    
    @abstractmethod
    def execute_plan_async(
        self, 
        plan: QueryPlan,
        progress_callback: Optional[Callable[[ExecutionProgress], None]] = None
    ) -> PlanExecutionResult:
        """
        Executa plano de forma assíncrona com callback de progresso
        
        Args:
            plan: Plano a ser executado
            progress_callback: Função chamada para reportar progresso
            
        Returns:
            Resultado completo da execução
        """
        pass
    
    @abstractmethod
    def resume_execution(self, plan_id: str) -> PlanExecutionResult:
        """
        Resume execução de um plano a partir do estado salvo
        
        Args:
            plan_id: ID do plano a ser resumido
            
        Returns:
            Resultado da execução resumida
        """
        pass
    
    @abstractmethod
    def cancel_execution(self, plan_id: str) -> bool:
        """
        Cancela execução em andamento
        
        Args:
            plan_id: ID do plano a ser cancelado
            
        Returns:
            True se cancelado com sucesso
        """
        pass
    
    @abstractmethod
    def get_execution_progress(self, plan_id: str) -> Optional[ExecutionProgress]:
        """
        Obtém progresso atual de uma execução
        
        Args:
            plan_id: ID do plano
            
        Returns:
            Progresso atual ou None se não encontrado
        """
        pass
    
    @abstractmethod
    def rollback_execution(self, plan_id: str, rollback_to_step: int) -> bool:
        """
        Faz rollback da execução até uma etapa específica
        
        Args:
            plan_id: ID do plano
            rollback_to_step: ID da etapa para rollback
            
        Returns:
            True se rollback bem-sucedido
        """
        pass


class IFallbackHandler(ABC):
    """
    Interface para tratamento de fallbacks em caso de falha
    """
    
    @abstractmethod
    def handle_step_failure(
        self,
        failed_step: QueryStep,
        error: Exception,
        context: StepExecutionContext
    ) -> Optional[StepExecutionResult]:
        """
        Trata falha de uma etapa específica
        
        Args:
            failed_step: Etapa que falhou
            error: Erro ocorrido
            context: Contexto da execução
            
        Returns:
            Resultado alternativo ou None se não foi possível recuperar
        """
        pass
    
    @abstractmethod
    def suggest_plan_modification(
        self,
        plan: QueryPlan,
        failed_step_id: int,
        error: Exception
    ) -> Optional[QueryPlan]:
        """
        Sugere modificação no plano para contornar falha
        
        Args:
            plan: Plano original
            failed_step_id: ID da etapa que falhou
            error: Erro ocorrido
            
        Returns:
            Plano modificado ou None se não foi possível
        """
        pass
    
    @abstractmethod
    def can_continue_without_step(
        self,
        plan: QueryPlan,
        failed_step_id: int
    ) -> bool:
        """
        Verifica se execução pode continuar sem uma etapa específica
        
        Args:
            plan: Plano de execução
            failed_step_id: ID da etapa que falhou
            
        Returns:
            True se pode continuar sem a etapa
        """
        pass


# Exceptions específicas do módulo
class ExecutionOrchestratorError(Exception):
    """Erro base para orquestração de execução"""
    pass


class StepExecutionError(ExecutionOrchestratorError):
    """Erro na execução de uma etapa específica"""
    def __init__(self, step_id: int, message: str, original_error: Optional[Exception] = None):
        self.step_id = step_id
        self.original_error = original_error
        super().__init__(f"Erro na etapa {step_id}: {message}")


class PlanExecutionTimeoutError(ExecutionOrchestratorError):
    """Erro de timeout na execução do plano"""
    pass


class DependencyNotMetError(ExecutionOrchestratorError):
    """Erro quando dependências de uma etapa não foram atendidas"""
    pass


class ExecutionStateError(ExecutionOrchestratorError):
    """Erro no gerenciamento de estado da execução"""
    pass