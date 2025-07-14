"""
Intelligent Cache Manager for Query Decomposition System
Sistema de cache multi-nível para otimização de performance
"""
import time
import hashlib
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import RLock
from collections import OrderedDict
import weakref

from domain.entities.query_decomposition import (
    QueryPlan,
    ComplexityAnalysis,
    PlanExecutionResult,
    QueryComplexityLevel
)


@dataclass
class CacheEntry:
    """Entrada do cache com metadata"""
    key: str
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    hit_count: int = 0
    size_bytes: int = 0
    ttl_seconds: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_expired(self) -> bool:
        """Verifica se entrada expirou"""
        if self.ttl_seconds is None:
            return False
        return (datetime.now() - self.created_at).total_seconds() > self.ttl_seconds
    
    @property
    def age_seconds(self) -> float:
        """Idade da entrada em segundos"""
        return (datetime.now() - self.created_at).total_seconds()
    
    def touch(self):
        """Atualiza last_accessed e incrementa contadores"""
        self.last_accessed = datetime.now()
        self.access_count += 1
        self.hit_count += 1


@dataclass
class CacheConfiguration:
    """Configuração do sistema de cache"""
    # Tamanhos máximos por nível
    max_query_plans: int = 1000
    max_complexity_analysis: int = 5000
    max_execution_results: int = 500
    max_template_matches: int = 2000
    
    # TTL por tipo de cache (em segundos)
    query_plan_ttl: float = 3600.0  # 1 hora
    complexity_analysis_ttl: float = 7200.0  # 2 horas
    execution_result_ttl: float = 1800.0  # 30 minutos
    template_match_ttl: float = 14400.0  # 4 horas
    
    # Configurações de limpeza
    cleanup_interval_seconds: float = 300.0  # 5 minutos
    max_memory_mb: float = 100.0  # 100MB total
    eviction_batch_size: int = 50
    
    # Configurações de performance
    enable_compression: bool = True
    enable_async_writes: bool = True
    enable_statistics: bool = True
    cache_hit_threshold: float = 0.7  # 70% hit rate target


class IntelligentCacheManager:
    """
    Sistema de cache multi-nível inteligente para decomposição de queries
    """
    
    def __init__(self, config: Optional[CacheConfiguration] = None):
        self.config = config or CacheConfiguration()
        self.logger = logging.getLogger(__name__)
        
        # Caches por tipo
        self._query_plan_cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._complexity_cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._execution_cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._template_cache: OrderedDict[str, CacheEntry] = OrderedDict()
        
        # Locks para thread safety
        self._plan_lock = RLock()
        self._complexity_lock = RLock()
        self._execution_lock = RLock()
        self._template_lock = RLock()
        
        # Estatísticas
        self._statistics = {
            "query_plans": {"hits": 0, "misses": 0, "evictions": 0},
            "complexity_analysis": {"hits": 0, "misses": 0, "evictions": 0},
            "execution_results": {"hits": 0, "misses": 0, "evictions": 0},
            "template_matches": {"hits": 0, "misses": 0, "evictions": 0},
            "total_memory_bytes": 0,
            "last_cleanup": datetime.now(),
            "cleanup_count": 0
        }
        
        # Inicializar limpeza automática
        self._start_cleanup_timer()
        
        self.logger.info("IntelligentCacheManager inicializado")
    
    # Cache de Query Plans
    
    def get_query_plan(self, query: str, complexity_threshold: float) -> Optional[QueryPlan]:
        """Recupera plano de query do cache"""
        cache_key = self._generate_plan_key(query, complexity_threshold)
        
        with self._plan_lock:
            if cache_key in self._query_plan_cache:
                entry = self._query_plan_cache[cache_key]
                
                if entry.is_expired:
                    del self._query_plan_cache[cache_key]
                    self._statistics["query_plans"]["misses"] += 1
                    return None
                
                entry.touch()
                # Move to end (LRU)
                self._query_plan_cache.move_to_end(cache_key)
                self._statistics["query_plans"]["hits"] += 1
                
                self.logger.debug(f"Cache hit para query plan: {cache_key[:16]}...")
                return entry.value
            
            self._statistics["query_plans"]["misses"] += 1
            return None
    
    def cache_query_plan(self, query: str, complexity_threshold: float, plan: QueryPlan) -> None:
        """Armazena plano de query no cache"""
        cache_key = self._generate_plan_key(query, complexity_threshold)
        
        with self._plan_lock:
            # Verificar limite do cache
            if len(self._query_plan_cache) >= self.config.max_query_plans:
                self._evict_lru_entries(self._query_plan_cache, "query_plans")
            
            entry = CacheEntry(
                key=cache_key,
                value=plan,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl_seconds=self.config.query_plan_ttl,
                size_bytes=self._estimate_size(plan),
                tags=["query_plan", f"strategy_{plan.strategy.value}"],
                metadata={
                    "complexity_score": plan.complexity_score,
                    "steps_count": len(plan.steps),
                    "strategy": plan.strategy.value
                }
            )
            
            self._query_plan_cache[cache_key] = entry
            self.logger.debug(f"Cached query plan: {cache_key[:16]}...")
    
    # Cache de Análise de Complexidade
    
    def get_complexity_analysis(self, query: str) -> Optional[ComplexityAnalysis]:
        """Recupera análise de complexidade do cache"""
        cache_key = self._generate_complexity_key(query)
        
        with self._complexity_lock:
            if cache_key in self._complexity_cache:
                entry = self._complexity_cache[cache_key]
                
                if entry.is_expired:
                    del self._complexity_cache[cache_key]
                    self._statistics["complexity_analysis"]["misses"] += 1
                    return None
                
                entry.touch()
                self._complexity_cache.move_to_end(cache_key)
                self._statistics["complexity_analysis"]["hits"] += 1
                
                return entry.value
            
            self._statistics["complexity_analysis"]["misses"] += 1
            return None
    
    def cache_complexity_analysis(self, query: str, analysis: ComplexityAnalysis) -> None:
        """Armazena análise de complexidade no cache"""
        cache_key = self._generate_complexity_key(query)
        
        with self._complexity_lock:
            if len(self._complexity_cache) >= self.config.max_complexity_analysis:
                self._evict_lru_entries(self._complexity_cache, "complexity_analysis")
            
            entry = CacheEntry(
                key=cache_key,
                value=analysis,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl_seconds=self.config.complexity_analysis_ttl,
                size_bytes=self._estimate_size(analysis),
                tags=["complexity", f"level_{analysis.complexity_level.value}"],
                metadata={
                    "complexity_score": analysis.complexity_score,
                    "complexity_level": analysis.complexity_level.value,
                    "patterns_count": len(analysis.patterns_detected)
                }
            )
            
            self._complexity_cache[cache_key] = entry
    
    # Cache de Resultados de Execução
    
    def get_execution_result(self, plan_id: str) -> Optional[PlanExecutionResult]:
        """Recupera resultado de execução do cache"""
        cache_key = f"exec_{plan_id}"
        
        with self._execution_lock:
            if cache_key in self._execution_cache:
                entry = self._execution_cache[cache_key]
                
                if entry.is_expired:
                    del self._execution_cache[cache_key]
                    self._statistics["execution_results"]["misses"] += 1
                    return None
                
                entry.touch()
                self._execution_cache.move_to_end(cache_key)
                self._statistics["execution_results"]["hits"] += 1
                
                return entry.value
            
            self._statistics["execution_results"]["misses"] += 1
            return None
    
    def cache_execution_result(self, result: PlanExecutionResult) -> None:
        """Armazena resultado de execução no cache"""
        cache_key = f"exec_{result.plan_id}"
        
        with self._execution_lock:
            if len(self._execution_cache) >= self.config.max_execution_results:
                self._evict_lru_entries(self._execution_cache, "execution_results")
            
            entry = CacheEntry(
                key=cache_key,
                value=result,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl_seconds=self.config.execution_result_ttl,
                size_bytes=self._estimate_size(result),
                tags=["execution", f"success_{result.success}"],
                metadata={
                    "success": result.success,
                    "steps_count": len(result.step_results),
                    "execution_time": result.total_execution_time,
                    "row_count": result.final_row_count
                }
            )
            
            self._execution_cache[cache_key] = entry
    
    # Cache de Template Matches
    
    def get_template_matches(self, query: str, complexity_score: float) -> Optional[List[Dict[str, Any]]]:
        """Recupera template matches do cache"""
        cache_key = self._generate_template_key(query, complexity_score)
        
        with self._template_lock:
            if cache_key in self._template_cache:
                entry = self._template_cache[cache_key]
                
                if entry.is_expired:
                    del self._template_cache[cache_key]
                    self._statistics["template_matches"]["misses"] += 1
                    return None
                
                entry.touch()
                self._template_cache.move_to_end(cache_key)
                self._statistics["template_matches"]["hits"] += 1
                
                return entry.value
            
            self._statistics["template_matches"]["misses"] += 1
            return None
    
    def cache_template_matches(
        self, 
        query: str, 
        complexity_score: float, 
        matches: List[Dict[str, Any]]
    ) -> None:
        """Armazena template matches no cache"""
        cache_key = self._generate_template_key(query, complexity_score)
        
        with self._template_lock:
            if len(self._template_cache) >= self.config.max_template_matches:
                self._evict_lru_entries(self._template_cache, "template_matches")
            
            entry = CacheEntry(
                key=cache_key,
                value=matches,
                created_at=datetime.now(),
                last_accessed=datetime.now(),
                ttl_seconds=self.config.template_match_ttl,
                size_bytes=self._estimate_size(matches),
                tags=["templates", f"matches_{len(matches)}"],
                metadata={
                    "matches_count": len(matches),
                    "complexity_score": complexity_score,
                    "best_score": matches[0].get("compatibility_score", 0) if matches else 0
                }
            )
            
            self._template_cache[cache_key] = entry
    
    # Métodos de limpeza e manutenção
    
    def clear_cache(self, cache_type: Optional[str] = None) -> Dict[str, int]:
        """Limpa cache especificado ou todos"""
        cleared_counts = {}
        
        if cache_type is None or cache_type == "query_plans":
            with self._plan_lock:
                cleared_counts["query_plans"] = len(self._query_plan_cache)
                self._query_plan_cache.clear()
        
        if cache_type is None or cache_type == "complexity_analysis":
            with self._complexity_lock:
                cleared_counts["complexity_analysis"] = len(self._complexity_cache)
                self._complexity_cache.clear()
        
        if cache_type is None or cache_type == "execution_results":
            with self._execution_lock:
                cleared_counts["execution_results"] = len(self._execution_cache)
                self._execution_cache.clear()
        
        if cache_type is None or cache_type == "template_matches":
            with self._template_lock:
                cleared_counts["template_matches"] = len(self._template_cache)
                self._template_cache.clear()
        
        self.logger.info(f"Cache cleared: {cleared_counts}")
        return cleared_counts
    
    def cleanup_expired_entries(self) -> Dict[str, int]:
        """Remove entradas expiradas de todos os caches"""
        cleanup_counts = {}
        
        # Query plans
        with self._plan_lock:
            expired_keys = [key for key, entry in self._query_plan_cache.items() if entry.is_expired]
            for key in expired_keys:
                del self._query_plan_cache[key]
            cleanup_counts["query_plans"] = len(expired_keys)
        
        # Complexity analysis
        with self._complexity_lock:
            expired_keys = [key for key, entry in self._complexity_cache.items() if entry.is_expired]
            for key in expired_keys:
                del self._complexity_cache[key]
            cleanup_counts["complexity_analysis"] = len(expired_keys)
        
        # Execution results
        with self._execution_lock:
            expired_keys = [key for key, entry in self._execution_cache.items() if entry.is_expired]
            for key in expired_keys:
                del self._execution_cache[key]
            cleanup_counts["execution_results"] = len(expired_keys)
        
        # Template matches
        with self._template_lock:
            expired_keys = [key for key, entry in self._template_cache.items() if entry.is_expired]
            for key in expired_keys:
                del self._template_cache[key]
            cleanup_counts["template_matches"] = len(expired_keys)
        
        self._statistics["last_cleanup"] = datetime.now()
        self._statistics["cleanup_count"] += 1
        
        total_cleaned = sum(cleanup_counts.values())
        if total_cleaned > 0:
            self.logger.info(f"Cleaned up {total_cleaned} expired entries: {cleanup_counts}")
        
        return cleanup_counts
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Retorna estatísticas detalhadas do cache"""
        stats = self._statistics.copy()
        
        # Adicionar estatísticas atuais
        stats["current_sizes"] = {
            "query_plans": len(self._query_plan_cache),
            "complexity_analysis": len(self._complexity_cache),
            "execution_results": len(self._execution_cache),
            "template_matches": len(self._template_cache)
        }
        
        # Calcular hit rates
        for cache_type in ["query_plans", "complexity_analysis", "execution_results", "template_matches"]:
            hits = stats[cache_type]["hits"]
            misses = stats[cache_type]["misses"]
            total = hits + misses
            stats[cache_type]["hit_rate"] = (hits / total * 100) if total > 0 else 0
        
        # Estimativa de memória
        stats["memory_estimate_mb"] = self._estimate_total_memory() / (1024 * 1024)
        
        return stats
    
    def optimize_cache(self) -> Dict[str, Any]:
        """Otimiza todos os caches removendo entradas menos úteis"""
        optimization_results = {}
        
        # Limpar entradas expiradas primeiro
        cleaned = self.cleanup_expired_entries()
        optimization_results["expired_cleaned"] = cleaned
        
        # Se ainda estamos acima do limite de memória, fazer eviction agressiva
        total_memory_mb = self._estimate_total_memory() / (1024 * 1024)
        
        if total_memory_mb > self.config.max_memory_mb:
            evicted = {}
            
            # Evict baseado em score de utilidade
            evicted["query_plans"] = self._evict_by_utility(self._query_plan_cache, self._plan_lock)
            evicted["complexity_analysis"] = self._evict_by_utility(self._complexity_cache, self._complexity_lock)
            evicted["execution_results"] = self._evict_by_utility(self._execution_cache, self._execution_lock)
            evicted["template_matches"] = self._evict_by_utility(self._template_cache, self._template_lock)
            
            optimization_results["low_utility_evicted"] = evicted
        
        optimization_results["final_memory_mb"] = self._estimate_total_memory() / (1024 * 1024)
        
        self.logger.info(f"Cache optimization completed: {optimization_results}")
        return optimization_results
    
    # Métodos auxiliares privados
    
    def _generate_plan_key(self, query: str, complexity_threshold: float) -> str:
        """Gera chave única para plano de query"""
        content = f"{query.lower().strip()}|{complexity_threshold}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _generate_complexity_key(self, query: str) -> str:
        """Gera chave única para análise de complexidade"""
        content = query.lower().strip()
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _generate_template_key(self, query: str, complexity_score: float) -> str:
        """Gera chave única para template matches"""
        content = f"{query.lower().strip()}|{complexity_score:.1f}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _estimate_size(self, obj: Any) -> int:
        """Estima tamanho de objeto em bytes"""
        try:
            # Estimativa simples baseada em serialização JSON
            if hasattr(obj, '__dict__'):
                return len(json.dumps(obj.__dict__, default=str).encode())
            else:
                return len(str(obj).encode())
        except:
            return 1024  # Fallback: 1KB
    
    def _estimate_total_memory(self) -> int:
        """Estima uso total de memória em bytes"""
        total = 0
        
        for cache in [self._query_plan_cache, self._complexity_cache, 
                     self._execution_cache, self._template_cache]:
            for entry in cache.values():
                total += entry.size_bytes
        
        return total
    
    def _evict_lru_entries(self, cache: OrderedDict, cache_type: str, count: Optional[int] = None) -> int:
        """Remove entradas LRU do cache"""
        if count is None:
            count = self.config.eviction_batch_size
        
        evicted = 0
        keys_to_remove = list(cache.keys())[:count]
        
        for key in keys_to_remove:
            if key in cache:
                del cache[key]
                evicted += 1
        
        self._statistics[cache_type]["evictions"] += evicted
        return evicted
    
    def _evict_by_utility(self, cache: OrderedDict, lock: RLock) -> int:
        """Remove entradas com menor score de utilidade"""
        with lock:
            if not cache:
                return 0
            
            # Calcular score de utilidade para cada entrada
            utility_scores = []
            for key, entry in cache.items():
                # Score baseado em: hit_count / age + recency
                age_hours = entry.age_seconds / 3600
                recency_score = 1.0 / (1 + (datetime.now() - entry.last_accessed).total_seconds() / 3600)
                hit_rate = entry.hit_count / max(1, entry.access_count)
                
                utility_score = (hit_rate * 10) + recency_score - (age_hours * 0.1)
                utility_scores.append((key, utility_score))
            
            # Ordenar por menor utilidade e remover os piores 25%
            utility_scores.sort(key=lambda x: x[1])
            to_remove = int(len(utility_scores) * 0.25)
            
            evicted = 0
            for key, _ in utility_scores[:to_remove]:
                if key in cache:
                    del cache[key]
                    evicted += 1
            
            return evicted
    
    def _start_cleanup_timer(self):
        """Inicia timer para limpeza automática"""
        def cleanup_task():
            while True:
                time.sleep(self.config.cleanup_interval_seconds)
                try:
                    self.cleanup_expired_entries()
                except Exception as e:
                    self.logger.error(f"Erro na limpeza automática: {e}")
        
        import threading
        cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        cleanup_thread.start()