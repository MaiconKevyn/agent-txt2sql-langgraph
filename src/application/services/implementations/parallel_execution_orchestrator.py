"""
Parallel Execution Orchestrator for Query Decomposition System
Sistema de execução paralela para otimização de performance
"""
import asyncio
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime
import threading
from queue import Queue, Empty

from domain.entities.query_decomposition import (
    QueryPlan,
    QueryStep,
    StepExecutionResult,
    PlanExecutionResult
)
# Import ExecutionProgress from the service layer
from application.services.execution_orchestrator_service import ExecutionProgress
from .intelligent_cache_manager import IntelligentCacheManager


@dataclass
class ParallelExecutionConfig:
    """Configuração para execução paralela"""
    max_workers: int = 4
    step_timeout_seconds: float = 30.0
    enable_step_caching: bool = True
    enable_result_streaming: bool = True
    batch_size: int = 2
    dependency_check_interval: float = 0.1
    memory_threshold_mb: float = 50.0
    enable_adaptive_parallelism: bool = True


@dataclass
class ParallelStepExecution:
    """Representa uma execução de step em paralelo"""
    step: QueryStep
    future: Optional[object] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[StepExecutionResult] = None
    worker_id: str = ""
    dependencies_ready: bool = False
    retry_count: int = 0
    max_retries: int = 2


@dataclass
class ExecutionBatch:
    """Batch de steps para execução paralela"""
    batch_id: str
    steps: List[ParallelStepExecution]
    dependencies: List[str] = field(default_factory=list)
    ready_for_execution: bool = False
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ParallelExecutionOrchestrator:
    """
    Orquestrador de execução paralela para decomposição de queries
    
    Funcionalidades:
    - Execução paralela de steps independentes
    - Gerenciamento de dependências entre steps
    - Cache inteligente de resultados
    - Streaming de resultados em tempo real
    - Adaptive parallelism baseado em recursos
    """
    
    def __init__(
        self, 
        query_service,
        cache_manager: Optional[IntelligentCacheManager] = None,
        config: Optional[ParallelExecutionConfig] = None
    ):
        self.query_service = query_service
        self.cache_manager = cache_manager or IntelligentCacheManager()
        self.config = config or ParallelExecutionConfig()
        self.logger = logging.getLogger(__name__)
        
        # Thread pool para execução paralela
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        
        # Controle de execução
        self._execution_lock = threading.RLock()
        self._result_queue = Queue()
        self._active_executions: Dict[str, ParallelStepExecution] = {}
        self._completed_steps: Dict[str, StepExecutionResult] = {}
        
        # Estatísticas de performance
        self._parallel_stats = {
            "total_executions": 0,
            "parallel_speedup": 0.0,
            "cache_hit_rate": 0.0,
            "avg_parallelism": 0.0,
            "resource_usage": {},
            "adaptive_adjustments": 0
        }
        
        self.logger.info("ParallelExecutionOrchestrator initialized")
    
    def execute_plan_parallel(
        self, 
        plan: QueryPlan,
        progress_callback: Optional[Callable[[ExecutionProgress], None]] = None
    ) -> PlanExecutionResult:
        """
        Executa plano com paralelização inteligente
        
        Args:
            plan: Plano de execução
            progress_callback: Callback para progresso
            
        Returns:
            Resultado da execução paralela
        """
        execution_start = datetime.now()
        plan_id = plan.plan_id
        
        try:
            self.logger.info(f"Starting parallel execution for plan {plan_id}")
            
            # Verificar cache primeiro
            cached_result = self.cache_manager.get_execution_result(plan_id)
            if cached_result:
                self.logger.info(f"Cache hit for plan {plan_id}")
                self._parallel_stats["cache_hit_rate"] += 1
                return cached_result
            
            # Analisar dependências e criar batches
            execution_batches = self._analyze_dependencies_and_create_batches(plan)
            
            # Determinar paralelismo adaptativo
            optimal_workers = self._calculate_optimal_parallelism(plan, execution_batches)
            
            # Configurar executor com paralelismo otimizado
            if optimal_workers != self.config.max_workers and self.config.enable_adaptive_parallelism:
                self._adjust_parallelism(optimal_workers)
                self._parallel_stats["adaptive_adjustments"] += 1
            
            # Executar batches sequencialmente, steps em paralelo
            all_results = []
            total_steps = len(plan.steps)
            completed_steps = 0
            
            for batch_idx, batch in enumerate(execution_batches):
                self.logger.debug(f"Executing batch {batch.batch_id} with {len(batch.steps)} steps")
                
                # Executar batch em paralelo
                batch_results = self._execute_batch_parallel(batch, progress_callback)
                all_results.extend(batch_results)
                
                # Atualizar progresso
                completed_steps += len(batch.steps)
                if progress_callback:
                    progress = ExecutionProgress(
                        plan_id=plan_id,
                        current_step=completed_steps,
                        total_steps=total_steps,
                        overall_progress=completed_steps / total_steps,
                        current_step_description=f"Batch {batch_idx + 1} completed"
                    )
                    progress_callback(progress)
                
                # Verificar se próximo batch pode ser executado
                self._update_dependency_status(execution_batches, batch_idx + 1)
            
            # Agregar resultados finais
            execution_time = (datetime.now() - execution_start).total_seconds()
            final_result = self._aggregate_parallel_results(plan, all_results, execution_time)
            
            # Cachear resultado
            if self.config.enable_step_caching and final_result.success:
                self.cache_manager.cache_execution_result(final_result)
            
            # Atualizar estatísticas
            self._update_parallel_statistics(plan, execution_time, len(execution_batches))
            
            self.logger.info(f"Parallel execution completed for plan {plan_id} in {execution_time:.2f}s")
            return final_result
            
        except Exception as e:
            execution_time = (datetime.now() - execution_start).total_seconds()
            self.logger.error(f"Parallel execution failed for plan {plan_id}: {e}")
            
            return PlanExecutionResult(
                plan_id=plan_id,
                success=False,
                step_results=[],
                completed_steps=[],
                failed_step_id=None,
                error_message=f"Parallel execution error: {str(e)}",
                total_execution_time=execution_time,
                final_results=[],
                final_row_count=0,
                metadata={
                    "execution_mode": "parallel",
                    "parallel_error": True,
                    "batches_created": len(execution_batches) if 'execution_batches' in locals() else 0
                }
            )
    
    def _analyze_dependencies_and_create_batches(self, plan: QueryPlan) -> List[ExecutionBatch]:
        """
        Analisa dependências entre steps e cria batches para execução paralela
        
        Args:
            plan: Plano de execução
            
        Returns:
            Lista de batches organizados por dependências
        """
        # Mapear dependências entre steps
        dependency_graph = {}
        for step in plan.steps:
            step_deps = []
            for other_step in plan.steps:
                if (other_step.step_id != step.step_id and 
                    self._has_dependency(step, other_step)):
                    step_deps.append(other_step.step_id)
            dependency_graph[step.step_id] = step_deps
        
        # Criar batches baseados em dependências
        batches = []
        remaining_steps = {step.step_id: step for step in plan.steps}
        batch_number = 0
        
        while remaining_steps:
            batch_number += 1
            batch_id = f"batch_{batch_number}_{int(time.time())}"
            
            # Encontrar steps sem dependências não resolvidas
            ready_steps = []
            for step_id, step in remaining_steps.items():
                dependencies = dependency_graph[step_id]
                if all(dep_id not in remaining_steps for dep_id in dependencies):
                    ready_steps.append(step)
            
            if not ready_steps:
                # Deadlock detectado - forçar execução sequencial do primeiro step
                self.logger.warning("Dependency deadlock detected, forcing sequential execution")
                ready_steps = [list(remaining_steps.values())[0]]
            
            # Criar batch com steps prontos
            parallel_steps = [
                ParallelStepExecution(step=step, dependencies_ready=True)
                for step in ready_steps
            ]
            
            batch = ExecutionBatch(
                batch_id=batch_id,
                steps=parallel_steps,
                ready_for_execution=True
            )
            batches.append(batch)
            
            # Remover steps processados
            for step in ready_steps:
                remaining_steps.pop(step.step_id)
        
        self.logger.info(f"Created {len(batches)} execution batches from {len(plan.steps)} steps")
        return batches
    
    def _has_dependency(self, step1: QueryStep, step2: QueryStep) -> bool:
        """
        Verifica se step1 depende de step2
        
        Args:
            step1: Step que pode ter dependência
            step2: Step que pode ser dependência
            
        Returns:
            True se step1 depende de step2
        """
        # Regras de dependência baseadas no tipo de step e descrição
        step1_desc = step1.description.lower()
        step2_desc = step2.description.lower()
        
        # Step de filtro não depende de agregação
        if "filtrar" in step1_desc and "agregar" in step2_desc:
            return False
        
        # Step de agregação depende de filtros
        if "agregar" in step1_desc and "filtrar" in step2_desc:
            return True
            
        # Step de ordenação depende de agregação
        if "ordenar" in step1_desc and "agregar" in step2_desc:
            return True
            
        # Step de limitação depende de ordenação
        if "limitar" in step1_desc and "ordenar" in step2_desc:
            return True
        
        # Por padrão, sem dependência se não há regra específica
        return False
    
    def _calculate_optimal_parallelism(self, plan: QueryPlan, batches: List[ExecutionBatch]) -> int:
        """
        Calcula paralelismo ótimo baseado no plano e recursos disponíveis
        
        Args:
            plan: Plano de execução
            batches: Batches criados
            
        Returns:
            Número ótimo de workers
        """
        if not self.config.enable_adaptive_parallelism:
            return self.config.max_workers
        
        # Calcular paralelismo baseado em:
        # 1. Número de steps por batch
        # 2. Complexidade estimada
        # 3. Recursos disponíveis
        
        max_batch_size = max(len(batch.steps) for batch in batches) if batches else 1
        complexity_factor = min(plan.complexity_score / 100.0, 1.0)
        
        # Paralelismo ideal baseado em batch size e complexidade
        ideal_workers = min(
            max_batch_size,  # Não exceder steps por batch
            int(self.config.max_workers * complexity_factor),  # Reduzir para queries simples
            self.config.max_workers  # Não exceder máximo configurado
        )
        
        # Mínimo de 1 worker
        return max(1, ideal_workers)
    
    def _adjust_parallelism(self, new_worker_count: int):
        """
        Ajusta número de workers do executor
        
        Args:
            new_worker_count: Novo número de workers
        """
        # ThreadPoolExecutor não permite ajuste dinâmico,
        # mas podemos controlar quantas tasks submetemos em paralelo
        self.config.max_workers = new_worker_count
        self.logger.info(f"Adjusted parallelism to {new_worker_count} workers")
    
    def _execute_batch_parallel(
        self, 
        batch: ExecutionBatch,
        progress_callback: Optional[Callable[[ExecutionProgress], None]] = None
    ) -> List[StepExecutionResult]:
        """
        Executa batch de steps em paralelo
        
        Args:
            batch: Batch para execução
            progress_callback: Callback para progresso
            
        Returns:
            Lista de resultados dos steps
        """
        batch.started_at = datetime.now()
        futures = {}
        
        try:
            # Submeter steps para execução paralela
            for parallel_step in batch.steps:
                step = parallel_step.step
                
                # Verificar cache primeiro
                cached_result = self._get_cached_step_result(step)
                if cached_result:
                    parallel_step.result = cached_result
                    parallel_step.completed_at = datetime.now()
                    continue
                
                # Submeter para executor
                future = self.executor.submit(self._execute_single_step_with_retry, step)
                parallel_step.future = future
                parallel_step.started_at = datetime.now()
                futures[future] = parallel_step
            
            # Aguardar conclusão com timeout
            results = []
            completed_count = 0
            
            for future in as_completed(futures.keys(), timeout=self.config.step_timeout_seconds):
                parallel_step = futures[future]
                step = parallel_step.step
                
                try:
                    result = future.result()
                    parallel_step.result = result
                    parallel_step.completed_at = datetime.now()
                    results.append(result)
                    
                    # Cachear resultado se bem-sucedido
                    if self.config.enable_step_caching and result.success:
                        self._cache_step_result(step, result)
                    
                    completed_count += 1
                    
                    # Atualizar progresso
                    if progress_callback:
                        progress = ExecutionProgress(
                            plan_id=batch.batch_id,
                            current_step=completed_count,
                            total_steps=len(batch.steps),
                            overall_progress=completed_count / len(batch.steps),
                            current_step_description=f"Step {step.step_id} completed"
                        )
                        progress_callback(progress)
                    
                except Exception as e:
                    # Falha na execução do step
                    failed_result = StepExecutionResult(
                        step_id=step.step_id,
                        success=False,
                        results=[],
                        execution_time=0.0,
                        row_count=0,
                        error_message=f"Parallel execution failed: {str(e)}",
                        metadata={"execution_mode": "parallel", "batch_id": batch.batch_id}
                    )
                    parallel_step.result = failed_result
                    results.append(failed_result)
                    
                    self.logger.error(f"Step {step.step_id} failed in parallel execution: {e}")
            
            batch.completed_at = datetime.now()
            return results
            
        except Exception as e:
            self.logger.error(f"Batch {batch.batch_id} execution failed: {e}")
            
            # Retornar resultados parciais ou vazios
            partial_results = []
            for parallel_step in batch.steps:
                if parallel_step.result:
                    partial_results.append(parallel_step.result)
                else:
                    # Criar resultado de erro
                    error_result = StepExecutionResult(
                        step_id=parallel_step.step.step_id,
                        success=False,
                        results=[],
                        execution_time=0.0,
                        row_count=0,
                        error_message=f"Batch execution error: {str(e)}",
                        metadata={"execution_mode": "parallel", "batch_error": True}
                    )
                    partial_results.append(error_result)
            
            return partial_results
    
    def _execute_single_step_with_retry(self, step: QueryStep) -> StepExecutionResult:
        """
        Executa step individual com retry
        
        Args:
            step: Step para execução
            
        Returns:
            Resultado da execução
        """
        max_retries = 2
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                # Simular execução usando query service
                # Em implementação real, seria o processamento específico do step
                request = self._convert_step_to_query_request(step)
                result = self.query_service.process_natural_language_query(request)
                
                # Converter QueryResult para StepExecutionResult
                return StepExecutionResult(
                    step_id=step.step_id,
                    success=result.success,
                    results=result.results,
                    execution_time=result.execution_time,
                    row_count=result.row_count,
                    error_message=result.error_message,
                    metadata={
                        "execution_mode": "parallel",
                        "attempt": attempt + 1,
                        "sql_query": result.sql_query
                    }
                )
                
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    self.logger.warning(f"Step {step.step_id} attempt {attempt + 1} failed, retrying: {e}")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                else:
                    self.logger.error(f"Step {step.step_id} failed after {max_retries + 1} attempts: {e}")
        
        # Retornar resultado de falha
        return StepExecutionResult(
            step_id=step.step_id,
            success=False,
            results=[],
            execution_time=0.0,
            row_count=0,
            error_message=f"Step failed after {max_retries + 1} attempts: {str(last_error)}",
            metadata={"execution_mode": "parallel", "retry_exhausted": True}
        )
    
    def _convert_step_to_query_request(self, step: QueryStep):
        """
        Converte QueryStep para QueryRequest para processamento
        
        Args:
            step: Step para conversão
            
        Returns:
            QueryRequest para processamento
        """
        from application.services.query_processing_service import QueryRequest
        
        return QueryRequest(
            user_query=step.description,
            session_id=f"parallel_{step.step_id}",
            timestamp=datetime.now(),
            metadata={"step_id": step.step_id, "execution_mode": "parallel"}
        )
    
    def _get_cached_step_result(self, step: QueryStep) -> Optional[StepExecutionResult]:
        """
        Recupera resultado de step do cache
        
        Args:
            step: Step para buscar no cache
            
        Returns:
            Resultado cacheado ou None
        """
        if not self.config.enable_step_caching:
            return None
        
        # Usar cache manager para buscar resultado
        cache_key = f"step_{step.step_id}_{hash(step.description)}"
        # Como cache manager não tem método específico para steps, 
        # usar cache de execution results com chave modificada
        return None  # Implementação simplificada
    
    def _cache_step_result(self, step: QueryStep, result: StepExecutionResult):
        """
        Cacheia resultado de step
        
        Args:
            step: Step executado
            result: Resultado para cachear
        """
        if not self.config.enable_step_caching:
            return
        
        # Implementação simplificada - em versão completa usaria cache específico para steps
        pass
    
    def _update_dependency_status(self, batches: List[ExecutionBatch], next_batch_idx: int):
        """
        Atualiza status de dependências para próximo batch
        
        Args:
            batches: Lista de batches
            next_batch_idx: Índice do próximo batch
        """
        if next_batch_idx >= len(batches):
            return
        
        # Marcar próximo batch como pronto se dependências foram satisfeitas
        next_batch = batches[next_batch_idx]
        next_batch.ready_for_execution = True
        
        self.logger.debug(f"Batch {next_batch.batch_id} ready for execution")
    
    def _aggregate_parallel_results(
        self, 
        plan: QueryPlan, 
        results: List[StepExecutionResult], 
        execution_time: float
    ) -> PlanExecutionResult:
        """
        Agrega resultados de execução paralela
        
        Args:
            plan: Plano original
            results: Resultados de todos os steps
            execution_time: Tempo total de execução
            
        Returns:
            Resultado agregado do plano
        """
        # Verificar sucesso geral
        success = all(result.success for result in results)
        
        # Agregar resultados finais (pegar do último step bem-sucedido)
        final_results = []
        final_row_count = 0
        
        if results:
            # Pegar resultados do último step bem-sucedido
            for result in reversed(results):
                if result.success and result.results:
                    final_results = result.results
                    final_row_count = result.row_count
                    break
        
        # Identificar step que falhou (se houver)
        failed_step_id = None
        error_message = None
        for result in results:
            if not result.success:
                failed_step_id = result.step_id
                error_message = result.error_message
                break
        
        # Criar resultado agregado
        return PlanExecutionResult(
            plan_id=plan.plan_id,
            success=success,
            step_results=results,
            completed_steps=[r.step_id for r in results if r.success],
            failed_step_id=failed_step_id,
            error_message=error_message,
            total_execution_time=execution_time,
            final_results=final_results,
            final_row_count=final_row_count,
            metadata={
                "execution_mode": "parallel",
                "total_steps": len(results),
                "successful_steps": sum(1 for r in results if r.success),
                "parallel_execution": True,
                "cache_enabled": self.config.enable_step_caching
            }
        )
    
    def _update_parallel_statistics(self, plan: QueryPlan, execution_time: float, batch_count: int):
        """
        Atualiza estatísticas de execução paralela
        
        Args:
            plan: Plano executado
            execution_time: Tempo de execução
            batch_count: Número de batches criados
        """
        self._parallel_stats["total_executions"] += 1
        
        # Estimar speedup (comparar com execução sequencial estimada)
        estimated_sequential_time = len(plan.steps) * 5.0  # 5s por step estimado
        speedup = estimated_sequential_time / execution_time if execution_time > 0 else 1.0
        
        # Atualizar média de speedup
        total_executions = self._parallel_stats["total_executions"]
        current_speedup = self._parallel_stats["parallel_speedup"]
        self._parallel_stats["parallel_speedup"] = (current_speedup * (total_executions - 1) + speedup) / total_executions
        
        # Calcular paralelismo médio
        avg_parallelism = len(plan.steps) / batch_count if batch_count > 0 else 1.0
        current_avg = self._parallel_stats["avg_parallelism"]
        self._parallel_stats["avg_parallelism"] = (current_avg * (total_executions - 1) + avg_parallelism) / total_executions
        
        self.logger.info(f"Parallel execution stats updated: speedup={speedup:.2f}x, avg_parallelism={avg_parallelism:.1f}")
    
    def get_parallel_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de execução paralela
        
        Returns:
            Estatísticas detalhadas
        """
        cache_stats = self.cache_manager.get_cache_statistics()
        
        return {
            "parallel_execution": self._parallel_stats.copy(),
            "cache_performance": {
                "hit_rate": cache_stats.get("execution_results", {}).get("hit_rate", 0),
                "cache_size": cache_stats.get("current_sizes", {}).get("execution_results", 0),
                "memory_usage_mb": cache_stats.get("memory_estimate_mb", 0)
            },
            "configuration": {
                "max_workers": self.config.max_workers,
                "step_timeout": self.config.step_timeout_seconds,
                "caching_enabled": self.config.enable_step_caching,
                "adaptive_parallelism": self.config.enable_adaptive_parallelism
            }
        }
    
    def shutdown(self):
        """Encerra executor e limpa recursos"""
        self.executor.shutdown(wait=True)
        self.logger.info("ParallelExecutionOrchestrator shutdown completed")