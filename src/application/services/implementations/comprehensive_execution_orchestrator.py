"""
Comprehensive Execution Orchestrator Implementation
Implementação completa do orquestrador de execução com otimizações de performance
"""
import time
import logging
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime

from domain.entities.query_decomposition import (
    QueryPlan,
    QueryStep,
    StepExecutionResult,
    PlanExecutionResult
)
from application.services.execution_orchestrator_service import (
    IExecutionOrchestrator,
    IStepExecutor,
    IResultAggregator,
    ExecutionProgress,
    StepExecutionContext,
    OrchestratorConfig,
    ExecutionOrchestratorError,
    StepExecutionError,
    PlanExecutionTimeoutError,
    DependencyNotMetError
)
from application.services.query_processing_service import IQueryProcessingService
from .step_executor import StepExecutor
from .result_aggregator import ResultAggregator
# NEW: Performance optimization components (Checkpoint 9)
from .intelligent_cache_manager import IntelligentCacheManager, CacheConfiguration
from .parallel_execution_orchestrator import ParallelExecutionOrchestrator, ParallelExecutionConfig


class ComprehensiveExecutionOrchestrator(IExecutionOrchestrator):
    """
    Implementação completa do orquestrador que executa planos de decomposição
    """
    
    def __init__(
        self,
        query_processing_service: IQueryProcessingService,
        config: Optional[OrchestratorConfig] = None,
        step_executor: Optional[IStepExecutor] = None,
        result_aggregator: Optional[IResultAggregator] = None,
        # NEW: Performance optimization parameters (Checkpoint 9)
        cache_manager: Optional[IntelligentCacheManager] = None,
        parallel_orchestrator: Optional[ParallelExecutionOrchestrator] = None,
        enable_performance_optimization: bool = True
    ):
        """
        Inicializa o orquestrador de execução com otimizações de performance
        
        Args:
            query_processing_service: Serviço de processamento de queries
            config: Configuração do orquestrador
            step_executor: Executor de etapas (opcional)
            result_aggregator: Agregador de resultados (opcional)
            cache_manager: Gerenciador de cache inteligente (opcional)
            parallel_orchestrator: Orquestrador de execução paralela (opcional)
            enable_performance_optimization: Habilita otimizações de performance
        """
        self.config = config or OrchestratorConfig()
        self.query_service = query_processing_service
        self.logger = logging.getLogger(__name__)
        
        # Inicializar componentes básicos
        self._step_executor = step_executor or StepExecutor(query_processing_service)
        self._result_aggregator = result_aggregator or ResultAggregator()
        
        # NEW: Performance optimization components (Checkpoint 9)
        self.enable_performance_optimization = enable_performance_optimization
        
        if enable_performance_optimization:
            # Inicializar cache manager
            cache_config = CacheConfiguration(
                max_query_plans=1000,
                max_execution_results=500,
                query_plan_ttl=3600.0,  # 1 hora
                execution_result_ttl=1800.0,  # 30 minutos
                enable_statistics=True
            )
            self.cache_manager = cache_manager or IntelligentCacheManager(cache_config)
            
            # Inicializar parallel orchestrator
            parallel_config = ParallelExecutionConfig(
                max_workers=4,
                step_timeout_seconds=30.0,
                enable_step_caching=True,
                enable_adaptive_parallelism=True
            )
            self.parallel_orchestrator = parallel_orchestrator or ParallelExecutionOrchestrator(
                query_processing_service, self.cache_manager, parallel_config
            )
            
            self.logger.info("Performance optimization enabled: cache + parallel execution")
        else:
            self.cache_manager = None
            self.parallel_orchestrator = None
            self.logger.info("Performance optimization disabled")
        
        # Estado da execução
        self._execution_states: Dict[str, Dict] = {}
        self._active_executions: Dict[str, bool] = {}
        
        # Estatísticas de performance
        self._performance_stats = {
            "cache_enabled_executions": 0,
            "parallel_enabled_executions": 0,
            "standard_executions": 0,
            "total_time_saved": 0.0,
            "avg_speedup": 1.0
        }
        
        self.logger.info("ComprehensiveExecutionOrchestrator inicializado com otimizações")
    
    def execute_plan(self, plan: QueryPlan) -> PlanExecutionResult:
        """
        Executa um plano de decomposição completo com otimizações de performance
        """
        start_time = time.time()
        plan_id = plan.plan_id
        
        try:
            self.logger.info(f"Iniciando execução do plano {plan_id} com {len(plan.steps)} etapas")
            
            # NEW: Verificar cache primeiro (Checkpoint 9)
            if self.enable_performance_optimization and self.cache_manager:
                cached_result = self.cache_manager.get_execution_result(plan_id)
                if cached_result:
                    self.logger.info(f"Cache hit para plano {plan_id}")
                    self._performance_stats["cache_enabled_executions"] += 1
                    return cached_result
            
            # Marcar execução como ativa
            self._active_executions[plan_id] = True
            
            # Inicializar estado da execução
            self._initialize_execution_state(plan)
            
            # NEW: Decidir estratégia de execução baseada na complexidade (Checkpoint 9)
            if (self.enable_performance_optimization and 
                self.parallel_orchestrator and 
                self._should_use_parallel_execution(plan)):
                
                self.logger.info(f"Usando execução paralela para plano {plan_id}")
                result = self.parallel_orchestrator.execute_plan_parallel(plan)
                self._performance_stats["parallel_enabled_executions"] += 1
                
                # Cachear resultado se bem-sucedido
                if self.cache_manager and result.success:
                    self.cache_manager.cache_execution_result(result)
                
                return result
            else:
                # Execução sequencial padrão
                self.logger.info(f"Usando execução sequencial para plano {plan_id}")
                result = self._execute_plan_sequential(plan)
                self._performance_stats["standard_executions"] += 1
                
                # Cachear resultado se bem-sucedido
                if self.enable_performance_optimization and self.cache_manager and result.success:
                    self.cache_manager.cache_execution_result(result)
                
                return result
                
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Erro na execução do plano {plan_id}: {e}")
            
            return PlanExecutionResult(
                plan_id=plan_id,
                success=False,
                step_results=[],
                completed_steps=[],
                failed_step_id=None,
                error_message=f"Erro na execução: {str(e)}",
                total_execution_time=execution_time,
                final_results=[],
                final_row_count=0,
                metadata={"execution_mode": "error", "error_type": type(e).__name__}
            )
    
    def _should_use_parallel_execution(self, plan: QueryPlan) -> bool:
        """
        Determina se deve usar execução paralela baseado na complexidade do plano
        
        Args:
            plan: Plano para análise
            
        Returns:
            True se deve usar execução paralela
        """
        # Usar paralela se:
        # 1. Plano tem mais de 2 steps
        # 2. Complexidade é alta (>= 50)
        # 3. Steps têm potencial para paralelização
        
        if len(plan.steps) < 2:
            return False
        
        if plan.complexity_score < 50.0:
            return False
        
        # Verificar se há steps que podem ser executados em paralelo
        # (steps de filtro podem ser paralelos, agregação deve ser sequencial)
        filter_steps = sum(1 for step in plan.steps if "filtrar" in step.description.lower())
        
        return filter_steps >= 2
    
    def _execute_plan_sequential(self, plan: QueryPlan) -> PlanExecutionResult:
        """
        Executa plano sequencialmente (método original)
        
        Args:
            plan: Plano para execução
            
        Returns:
            Resultado da execução sequencial
        """
        start_time = time.time()
        plan_id = plan.plan_id
        
        try:
            # Executar etapas em ordem de dependência
            step_results = []
            completed_steps = []
            failed_step_id = None
            
            execution_order = self._calculate_execution_order(plan.steps)
            
            for step in execution_order:
                if not self._active_executions.get(plan_id, False):
                    self.logger.warning(f"Execução do plano {plan_id} foi cancelada")
                    break
                
                try:
                    # Executar etapa
                    step_result = self._execute_single_step(step, plan, step_results)
                    step_results.append(step_result)
                    
                    if step_result.success:
                        completed_steps.append(step.step_id)
                        self.logger.info(f"Etapa {step.step_id} concluída com sucesso")
                    else:
                        failed_step_id = step.step_id
                        self.logger.error(f"Etapa {step.step_id} falhou: {step_result.error_message}")
                        
                        # Decidir se continuar ou parar
                        if self.config.execution_mode.value == "fail_fast":
                            break
                        elif self.config.execution_mode.value == "best_effort":
                            self.logger.warning(f"Continuando execução apesar da falha na etapa {step.step_id}")
                            continue
                
                except Exception as e:
                    error_msg = f"Erro inesperado na etapa {step.step_id}: {str(e)}"
                    self.logger.error(error_msg)
                    
                    step_result = StepExecutionResult(
                        step_id=step.step_id,
                        success=False,
                        sql_executed="",
                        results=[],
                        row_count=0,
                        execution_time=0.0,
                        error_message=error_msg
                    )
                    step_results.append(step_result)
                    failed_step_id = step.step_id
                    
                    if self.config.execution_mode.value == "fail_fast":
                        break
            
            # Combinar resultados finais
            try:
                combined_results = self._result_aggregator.combine_step_results(step_results, plan)
                final_row_count = len(combined_results)
                
                # Formatar resultado final
                formatted_result = self._result_aggregator.format_final_result(
                    combined_results, plan.original_query, plan
                )
                
            except Exception as e:
                self.logger.error(f"Erro ao combinar resultados: {e}")
                combined_results = []
                final_row_count = 0
                formatted_result = {"error": str(e)}
            
            total_execution_time = time.time() - start_time
            
            # Determinar sucesso geral
            successful_steps = [r for r in step_results if r.success]
            overall_success = len(successful_steps) > 0 and (
                failed_step_id is None or 
                self.config.execution_mode.value == "best_effort"
            )
            
            # Criar resultado do plano
            plan_result = PlanExecutionResult(
                plan_id=plan_id,
                success=overall_success,
                step_results=step_results,
                total_execution_time=total_execution_time,
                final_results=combined_results,
                final_row_count=final_row_count,
                completed_steps=completed_steps,
                failed_step_id=failed_step_id,
                error_message=step_results[-1].error_message if step_results and not overall_success else None,
                metadata={
                    "strategy": plan.strategy.value,
                    "execution_mode": self.config.execution_mode.value,
                    "formatted_result": formatted_result,
                    "steps_attempted": len(step_results),
                    "steps_completed": len(completed_steps)
                }
            )
            
            self.logger.info(
                f"Plano {plan_id} concluído: "
                f"sucesso={overall_success}, "
                f"etapas={len(completed_steps)}/{len(plan.steps)}, "
                f"tempo={total_execution_time:.2f}s"
            )
            
            return plan_result
            
        except Exception as e:
            total_execution_time = time.time() - start_time
            error_msg = f"Erro crítico na execução do plano {plan_id}: {str(e)}"
            self.logger.error(error_msg)
            
            return PlanExecutionResult(
                plan_id=plan_id,
                success=False,
                step_results=[],
                total_execution_time=total_execution_time,
                final_results=[],
                final_row_count=0,
                completed_steps=[],
                error_message=error_msg
            )
        
        finally:
            # Limpar estado da execução
            self._active_executions.pop(plan_id, None)
            self._execution_states.pop(plan_id, None)
    
    def execute_plan_async(
        self, 
        plan: QueryPlan,
        progress_callback: Optional[Callable[[ExecutionProgress], None]] = None
    ) -> PlanExecutionResult:
        """
        Executa plano de forma assíncrona com callback de progresso
        (Por enquanto, implementação síncrona com callbacks)
        """
        start_time = time.time()
        
        def update_progress(step_index: int, total_steps: int, current_step: QueryStep, message: str):
            if progress_callback:
                progress = ExecutionProgress(
                    plan_id=plan.plan_id,
                    total_steps=total_steps,
                    completed_steps=step_index,
                    failed_steps=0,
                    current_step_id=current_step.step_id,
                    current_step_description=current_step.description,
                    elapsed_time=time.time() - start_time,
                    estimated_remaining_time=None,
                    overall_progress=step_index / total_steps
                )
                progress_callback(progress)
        
        # Executar com callbacks de progresso
        plan_copy = plan
        execution_order = self._calculate_execution_order(plan.steps)
        
        for i, step in enumerate(execution_order):
            update_progress(i, len(execution_order), step, f"Executando etapa {step.step_id}")
        
        # Executar o plano normalmente
        result = self.execute_plan(plan)
        
        # Callback final
        if progress_callback:
            final_progress = ExecutionProgress(
                plan_id=plan.plan_id,
                total_steps=len(plan.steps),
                completed_steps=len(result.completed_steps),
                failed_steps=len(plan.steps) - len(result.completed_steps),
                current_step_id=None,
                current_step_description="Concluído",
                elapsed_time=result.total_execution_time,
                estimated_remaining_time=0.0,
                overall_progress=1.0
            )
            progress_callback(final_progress)
        
        return result
    
    def resume_execution(self, plan_id: str) -> PlanExecutionResult:
        """
        Resume execução de um plano a partir do estado salvo
        (Implementação básica - para versões futuras)
        """
        raise NotImplementedError("Resume execution será implementado em versão futura")
    
    def cancel_execution(self, plan_id: str) -> bool:
        """
        Cancela execução em andamento
        """
        if plan_id in self._active_executions:
            self._active_executions[plan_id] = False
            self.logger.info(f"Execução do plano {plan_id} cancelada")
            return True
        return False
    
    def get_execution_progress(self, plan_id: str) -> Optional[ExecutionProgress]:
        """
        Obtém progresso atual de uma execução
        (Implementação básica)
        """
        if plan_id in self._execution_states:
            state = self._execution_states[plan_id]
            return ExecutionProgress(
                plan_id=plan_id,
                total_steps=state.get("total_steps", 0),
                completed_steps=state.get("completed_steps", 0),
                failed_steps=state.get("failed_steps", 0),
                current_step_id=state.get("current_step_id"),
                current_step_description=state.get("current_step_description", ""),
                elapsed_time=time.time() - state.get("start_time", time.time()),
                estimated_remaining_time=None,
                overall_progress=state.get("progress", 0.0)
            )
        return None
    
    def rollback_execution(self, plan_id: str, rollback_to_step: int) -> bool:
        """
        Faz rollback da execução até uma etapa específica
        (Implementação futura)
        """
        raise NotImplementedError("Rollback será implementado em versão futura")
    
    # NEW: Performance monitoring methods (Checkpoint 9)
    
    def get_performance_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas detalhadas de performance
        
        Returns:
            Estatísticas de cache, paralelização e performance geral
        """
        stats = {
            "execution_statistics": self._performance_stats.copy(),
            "cache_statistics": None,
            "parallel_statistics": None,
            "optimization_enabled": self.enable_performance_optimization
        }
        
        # Adicionar estatísticas de cache se disponível
        if self.cache_manager:
            stats["cache_statistics"] = self.cache_manager.get_cache_statistics()
        
        # Adicionar estatísticas de paralelização se disponível
        if self.parallel_orchestrator:
            stats["parallel_statistics"] = self.parallel_orchestrator.get_parallel_statistics()
        
        return stats
    
    def optimize_performance(self) -> Dict[str, Any]:
        """
        Executa otimização de performance (limpeza de cache, etc.)
        
        Returns:
            Resultado das otimizações aplicadas
        """
        optimization_results = {}
        
        if self.cache_manager:
            cache_optimization = self.cache_manager.optimize_cache()
            optimization_results["cache_optimization"] = cache_optimization
        
        # Calcular speedup médio
        total_executions = sum(self._performance_stats.values()) - self._performance_stats["total_time_saved"] - self._performance_stats["avg_speedup"]
        if total_executions > 0:
            parallel_executions = self._performance_stats["parallel_enabled_executions"]
            cache_executions = self._performance_stats["cache_enabled_executions"]
            
            # Estimativa de speedup baseada no tipo de execução
            estimated_speedup = 1.0
            if parallel_executions > 0:
                estimated_speedup += (parallel_executions / total_executions) * 2.0  # 2x speedup estimado para paralela
            if cache_executions > 0:
                estimated_speedup += (cache_executions / total_executions) * 5.0  # 5x speedup estimado para cache
            
            self._performance_stats["avg_speedup"] = estimated_speedup
            optimization_results["performance_metrics"] = {
                "avg_speedup": estimated_speedup,
                "optimization_rate": (parallel_executions + cache_executions) / total_executions * 100
            }
        
        return optimization_results
    
    def enable_performance_monitoring(self, enabled: bool = True):
        """
        Habilita/desabilita monitoramento de performance
        
        Args:
            enabled: True para habilitar, False para desabilitar
        """
        if enabled and not self.enable_performance_optimization:
            self.logger.info("Performance monitoring cannot be enabled without performance optimization")
            return
        
        # Performance monitoring está sempre ativo quando optimization está habilitada
        self.logger.info(f"Performance monitoring: {'enabled' if enabled else 'disabled'}")
    
    def get_cache_hit_rate(self) -> float:
        """
        Retorna taxa de acerto do cache
        
        Returns:
            Taxa de acerto como percentual (0-100)
        """
        if not self.cache_manager:
            return 0.0
        
        cache_stats = self.cache_manager.get_cache_statistics()
        execution_stats = cache_stats.get("execution_results", {})
        return execution_stats.get("hit_rate", 0.0)
    
    def get_parallel_efficiency(self) -> float:
        """
        Retorna eficiência da execução paralela
        
        Returns:
            Eficiência como ratio (0-1)
        """
        if not self.parallel_orchestrator:
            return 0.0
        
        parallel_stats = self.parallel_orchestrator.get_parallel_statistics()
        parallel_execution = parallel_stats.get("parallel_execution", {})
        speedup = parallel_execution.get("parallel_speedup", 1.0)
        
        # Eficiência = speedup / número_de_workers
        max_workers = parallel_stats.get("configuration", {}).get("max_workers", 1)
        return min(1.0, speedup / max_workers)
    
    def _initialize_execution_state(self, plan: QueryPlan):
        """Inicializa estado da execução"""
        self._execution_states[plan.plan_id] = {
            "start_time": time.time(),
            "total_steps": len(plan.steps),
            "completed_steps": 0,
            "failed_steps": 0,
            "current_step_id": None,
            "progress": 0.0
        }
    
    def _calculate_execution_order(self, steps: List[QueryStep]) -> List[QueryStep]:
        """
        Calcula ordem de execução baseada nas dependências
        """
        # Implementação simples: ordenar por step_id (já respeitará dependências se bem formado)
        return sorted(steps, key=lambda s: s.step_id)
    
    def _execute_single_step(
        self, 
        step: QueryStep, 
        plan: QueryPlan, 
        previous_results: List[StepExecutionResult]
    ) -> StepExecutionResult:
        """
        Executa uma única etapa
        """
        step_start_time = time.time()
        
        try:
            # Verificar timeout da etapa
            if time.time() - step_start_time > self.config.step_timeout_seconds:
                raise TimeoutError(f"Timeout na etapa {step.step_id}")
            
            # Preparar contexto da execução
            previous_results_dict = {r.step_id: r for r in previous_results}
            
            context = StepExecutionContext(
                step=step,
                plan=plan,
                previous_results=previous_results_dict,
                execution_variables=self._build_execution_variables(plan, step),
                retry_count=0,
                start_time=step_start_time
            )
            
            # Executar etapa com retry
            last_error = None
            for attempt in range(self.config.max_retries_per_step + 1):
                try:
                    context.retry_count = attempt
                    result = self._step_executor.execute_step(context)
                    
                    if result.success:
                        return result
                    else:
                        last_error = result.error_message
                        if attempt < self.config.max_retries_per_step:
                            self.logger.warning(
                                f"Tentativa {attempt + 1} falhou para etapa {step.step_id}, "
                                f"tentando novamente em {self.config.retry_delay_seconds}s"
                            )
                            time.sleep(self.config.retry_delay_seconds)
                        
                except Exception as e:
                    last_error = str(e)
                    if attempt < self.config.max_retries_per_step:
                        self.logger.warning(f"Erro na tentativa {attempt + 1}: {e}")
                        time.sleep(self.config.retry_delay_seconds)
                    else:
                        raise
            
            # Se chegou aqui, todas as tentativas falharam
            execution_time = time.time() - step_start_time
            return StepExecutionResult(
                step_id=step.step_id,
                success=False,
                sql_executed=step.sql_template,
                results=[],
                row_count=0,
                execution_time=execution_time,
                error_message=f"Falha após {self.config.max_retries_per_step + 1} tentativas: {last_error}"
            )
            
        except Exception as e:
            execution_time = time.time() - step_start_time
            error_msg = f"Erro crítico na etapa {step.step_id}: {str(e)}"
            self.logger.error(error_msg)
            
            return StepExecutionResult(
                step_id=step.step_id,
                success=False,
                sql_executed=step.sql_template,
                results=[],
                row_count=0,
                execution_time=execution_time,
                error_message=error_msg
            )
    
    def _build_execution_variables(self, plan: QueryPlan, step: QueryStep) -> Dict[str, any]:
        """
        Constrói variáveis de execução para uma etapa
        """
        variables = {
            "plan_id": plan.plan_id,
            "step_id": step.step_id,
            "original_query": plan.original_query,
            "strategy": plan.strategy.value
        }
        
        # Adicionar variáveis específicas da query
        query_lower = plan.original_query.lower()
        
        # Detectar sexo
        if any(term in query_lower for term in ['mulher', 'feminino']):
            variables['sexo'] = 'feminino'
            variables['sexo_code'] = '3'
        elif any(term in query_lower for term in ['homem', 'masculino']):
            variables['sexo'] = 'masculino'
            variables['sexo_code'] = '1'
        
        # Detectar tipo de doença
        if any(term in query_lower for term in ['respiratória', 'respiratorio']):
            variables['doenca_tipo'] = 'respiratoria'
            variables['doenca_pattern'] = 'J%'
        elif any(term in query_lower for term in ['cardíaca', 'coração']):
            variables['doenca_tipo'] = 'cardiaca'
            variables['doenca_pattern'] = 'I%'
        
        return variables
    
    def get_orchestrator_statistics(self) -> Dict[str, any]:
        """
        Retorna estatísticas do orquestrador
        """
        return {
            "config": {
                "execution_mode": self.config.execution_mode.value,
                "max_retries_per_step": self.config.max_retries_per_step,
                "step_timeout_seconds": self.config.step_timeout_seconds,
                "total_timeout_seconds": self.config.total_timeout_seconds
            },
            "active_executions": len(self._active_executions),
            "execution_states": len(self._execution_states),
            "components": {
                "step_executor": type(self._step_executor).__name__,
                "result_aggregator": type(self._result_aggregator).__name__
            }
        }