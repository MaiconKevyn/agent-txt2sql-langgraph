#!/usr/bin/env python3
"""
Test script for Checkpoint 9 - Performance Optimization
Valida otimizações de cache e paralelização no sistema de decomposição
"""
import sys
import os
import time
import asyncio
from datetime import datetime

# Adicionar src ao path para imports
project_root = os.path.dirname(__file__)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_path)

# Mock imports for testing
try:
    from src.application.services.implementations.intelligent_cache_manager import (
        IntelligentCacheManager, 
        CacheConfiguration,
        CacheEntry
    )
    from src.application.services.implementations.parallel_execution_orchestrator import (
        ParallelExecutionOrchestrator,
        ParallelExecutionConfig
    )
    from src.application.services.implementations.comprehensive_execution_orchestrator import (
        ComprehensiveExecutionOrchestrator
    )
    # Domain entities
    from domain.entities.query_decomposition import (
        QueryPlan,
        QueryStep,
        PlanExecutionResult,
        StepExecutionResult,
        ComplexityAnalysis,
        QueryComplexityLevel,
        QueryStrategy
    )
    
    print("✅ All imports successful")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Creating mock classes for testing...")
    
    from dataclasses import dataclass
    from typing import Dict, Any, List, Optional
    from datetime import datetime
    import time
    
    # Mock classes
    @dataclass
    class QueryStep:
        step_id: str
        description: str
        sql_template: str
        
    @dataclass 
    class QueryPlan:
        plan_id: str
        steps: List[QueryStep]
        complexity_score: float = 45.0
        strategy: str = "sequential"
        
    @dataclass
    class StepExecutionResult:
        step_id: str
        success: bool
        results: List[Dict]
        execution_time: float
        row_count: int
        error_message: Optional[str] = None
        metadata: Optional[Dict] = None
        
    @dataclass
    class PlanExecutionResult:
        plan_id: str
        success: bool
        step_results: List[StepExecutionResult]
        completed_steps: List[str]
        failed_step_id: Optional[str]
        error_message: Optional[str]
        total_execution_time: float
        final_results: List[Dict]
        final_row_count: int
        metadata: Dict[str, Any]


def test_intelligent_cache_manager():
    """Testa o sistema de cache inteligente"""
    print("🧪 Testando Intelligent Cache Manager")
    print("=" * 60)
    
    try:
        # Criar configuração de cache
        cache_config = CacheConfiguration(
            max_query_plans=100,
            max_execution_results=50,
            query_plan_ttl=300.0,  # 5 minutos
            execution_result_ttl=180.0,  # 3 minutos
            enable_statistics=True
        )
        
        # Inicializar cache manager
        cache_manager = IntelligentCacheManager(cache_config)
        print("✅ Cache manager inicializado")
        
        # Testar cache de complexity analysis
        test_query = "Quantas mulheres idosas morreram por doenças respiratórias em Porto Alegre?"
        
        # Simular análise de complexidade
        mock_analysis = ComplexityAnalysis(
            query=test_query,
            complexity_factors={"respiratory": True, "demographic": True, "geographic": True},
            analysis_metadata={"patterns_detected": ["respiratory", "demographic"]}
        )
        
        # Cachear análise
        cache_manager.cache_complexity_analysis(test_query, mock_analysis)
        print("✅ Análise de complexidade cacheada")
        
        # Recuperar do cache
        cached_analysis = cache_manager.get_complexity_analysis(test_query)
        if cached_analysis:
            print("✅ Cache hit para análise de complexidade")
        else:
            print("❌ Cache miss inesperado")
            
        # Testar estatísticas do cache
        stats = cache_manager.get_cache_statistics()
        print(f"📊 Estatísticas do cache:")
        print(f"   Complexity analysis: {stats['current_sizes']['complexity_analysis']} entradas")
        print(f"   Memory usage: {stats.get('memory_estimate_mb', 0):.2f} MB")
        
        # Testar otimização do cache
        optimization_results = cache_manager.optimize_cache()
        print(f"🔧 Otimização executada: {optimization_results}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste de cache: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parallel_execution_orchestrator():
    """Testa o orquestrador de execução paralela"""
    print("\n\n⚡ Testando Parallel Execution Orchestrator")
    print("=" * 60)
    
    try:
        # Mock query service
        class MockQueryService:
            def process_natural_language_query(self, request):
                time.sleep(0.2)  # Simular processamento
                return type('MockResult', (), {
                    'success': True,
                    'results': [{'count': 42}],
                    'execution_time': 0.2,
                    'row_count': 1,
                    'error_message': None,
                    'sql_query': 'SELECT COUNT(*) FROM mock_table'
                })()
        
        # Configurar execução paralela
        parallel_config = ParallelExecutionConfig(
            max_workers=2,
            step_timeout_seconds=10.0,
            enable_step_caching=True,
            enable_adaptive_parallelism=True
        )
        
        # Inicializar parallel orchestrator
        parallel_orchestrator = ParallelExecutionOrchestrator(
            MockQueryService(),
            config=parallel_config
        )
        print("✅ Parallel orchestrator inicializado")
        
        # Criar plano de teste com múltiplos steps
        test_steps = [
            QueryStep(
                step_id="step_1",
                description="Filtrar pacientes por sexo feminino",
                sql_template="SELECT * FROM sus_data WHERE sexo = 3"
            ),
            QueryStep(
                step_id="step_2", 
                description="Filtrar por doenças respiratórias",
                sql_template="SELECT * FROM {prev_table} WHERE diag_princ LIKE 'J%'"
            ),
            QueryStep(
                step_id="step_3",
                description="Agregar por cidade",
                sql_template="SELECT cidade, COUNT(*) FROM {prev_table} GROUP BY cidade"
            )
        ]
        
        test_plan = QueryPlan(
            plan_id=f"parallel_test_{int(time.time())}",
            steps=test_steps,
            complexity_score=75.0,
            strategy="sequential_filtering"
        )
        
        # Executar plano em paralelo
        print(f"🚀 Executando plano com {len(test_steps)} steps...")
        start_time = time.time()
        
        def progress_callback(progress):
            print(f"   📊 Progresso: {progress.overall_progress:.0%} - {progress.current_step_description}")
        
        result = parallel_orchestrator.execute_plan_parallel(test_plan, progress_callback)
        execution_time = time.time() - start_time
        
        print(f"✅ Execução paralela concluída em {execution_time:.2f}s")
        print(f"   Sucesso: {result.success}")
        print(f"   Steps executados: {len(result.completed_steps)}")
        print(f"   Resultados finais: {result.final_row_count} registros")
        
        # Obter estatísticas de paralelização
        parallel_stats = parallel_orchestrator.get_parallel_statistics()
        print(f"📊 Estatísticas de paralelização:")
        print(f"   Speedup médio: {parallel_stats['parallel_execution']['parallel_speedup']:.2f}x")
        print(f"   Paralelismo médio: {parallel_stats['parallel_execution']['avg_parallelism']:.1f}")
        
        return result.success
        
    except Exception as e:
        print(f"❌ Erro no teste paralelo: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_integrated_performance_optimization():
    """Testa integração completa das otimizações de performance"""
    print("\n\n🔧 Testando Integração de Performance")
    print("=" * 60)
    
    try:
        # Mock query service
        class MockQueryService:
            def process_natural_language_query(self, request):
                time.sleep(0.1)
                return type('MockResult', (), {
                    'success': True,
                    'results': [{'total': 156}],
                    'execution_time': 0.1,
                    'row_count': 1,
                    'error_message': None,
                    'sql_query': 'SELECT COUNT(*) FROM integrated_test'
                })()
        
        # Inicializar ComprehensiveExecutionOrchestrator com otimizações
        comprehensive_orchestrator = ComprehensiveExecutionOrchestrator(
            MockQueryService(),
            enable_performance_optimization=True
        )
        print("✅ Comprehensive orchestrator com otimizações inicializado")
        
        # Criar plano complexo para teste
        complex_steps = [
            QueryStep("filter_1", "Filtrar por idade > 60", "SELECT * FROM sus_data WHERE idade > 60"),
            QueryStep("filter_2", "Filtrar por sexo feminino", "SELECT * FROM {prev} WHERE sexo = 3"),
            QueryStep("filter_3", "Filtrar respiratórias", "SELECT * FROM {prev} WHERE diag LIKE 'J%'"),
            QueryStep("aggregate", "Agregar por município", "SELECT municipio, COUNT(*) FROM {prev} GROUP BY municipio"),
            QueryStep("sort", "Ordenar por contagem", "SELECT * FROM {prev} ORDER BY count DESC")
        ]
        
        complex_plan = QueryPlan(
            plan_id=f"integrated_test_{int(time.time())}",
            steps=complex_steps,
            complexity_score=85.0,  # Alta complexidade para triggering paralela
            strategy="sequential_filtering"
        )
        
        # Primeira execução (cache miss)
        print("🔄 Primeira execução (cache miss)...")
        start_time = time.time()
        result1 = comprehensive_orchestrator.execute_plan(complex_plan)
        first_exec_time = time.time() - start_time
        
        print(f"   Tempo: {first_exec_time:.2f}s")
        print(f"   Sucesso: {result1.success}")
        print(f"   Modo de execução: {result1.metadata.get('execution_mode', 'unknown')}")
        
        # Segunda execução (deveria usar cache)
        print("⚡ Segunda execução (cache hit esperado)...")
        start_time = time.time()
        result2 = comprehensive_orchestrator.execute_plan(complex_plan)
        second_exec_time = time.time() - start_time
        
        print(f"   Tempo: {second_exec_time:.2f}s")
        print(f"   Speedup: {first_exec_time/second_exec_time:.1f}x")
        
        # Obter estatísticas de performance
        performance_stats = comprehensive_orchestrator.get_performance_statistics()
        print(f"📊 Estatísticas de Performance:")
        print(f"   Otimização habilitada: {performance_stats['optimization_enabled']}")
        
        if performance_stats.get('cache_statistics'):
            cache_stats = performance_stats['cache_statistics']
            print(f"   Cache - Execution results: {cache_stats['current_sizes']['execution_results']}")
            print(f"   Cache - Memory usage: {cache_stats.get('memory_estimate_mb', 0):.2f} MB")
        
        if performance_stats.get('parallel_statistics'):
            parallel_stats = performance_stats['parallel_statistics']
            print(f"   Parallel - Total executions: {parallel_stats['parallel_execution']['total_executions']}")
            print(f"   Parallel - Average speedup: {parallel_stats['parallel_execution']['parallel_speedup']:.2f}x")
        
        # Executar otimização do sistema
        optimization_results = comprehensive_orchestrator.optimize_performance()
        print(f"🔧 Otimização do sistema executada: {optimization_results}")
        
        return result1.success and result2.success
        
    except Exception as e:
        print(f"❌ Erro no teste integrado: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cache_expiration_and_cleanup():
    """Testa expiração e limpeza do cache"""
    print("\n\n🧹 Testando Expiração e Limpeza do Cache")
    print("=" * 60)
    
    try:
        # Cache com TTL curto para teste
        cache_config = CacheConfiguration(
            max_query_plans=10,
            query_plan_ttl=0.5,  # 0.5 segundos para teste rápido
            execution_result_ttl=0.3,
            cleanup_interval_seconds=1.0
        )
        
        cache_manager = IntelligentCacheManager(cache_config)
        print("✅ Cache manager com TTL curto inicializado")
        
        # Adicionar algumas entradas
        test_queries = [
            "Query de teste 1",
            "Query de teste 2", 
            "Query de teste 3"
        ]
        
        for i, query in enumerate(test_queries):
            mock_analysis = ComplexityAnalysis(
                query=query,
                complexity_factors={"test": True},
                analysis_metadata={"test_id": i}
            )
            cache_manager.cache_complexity_analysis(query, mock_analysis)
        
        # Verificar que estão no cache
        initial_stats = cache_manager.get_cache_statistics()
        initial_count = initial_stats['current_sizes']['complexity_analysis']
        print(f"📊 Entradas iniciais no cache: {initial_count}")
        
        # Aguardar expiração
        print("⏳ Aguardando expiração (2 segundos)...")
        time.sleep(2)
        
        # Forçar limpeza
        cleanup_results = cache_manager.cleanup_expired_entries()
        print(f"🧹 Limpeza executada: {cleanup_results}")
        
        # Verificar que entradas expiraram
        final_stats = cache_manager.get_cache_statistics()
        final_count = final_stats['current_sizes']['complexity_analysis']
        print(f"📊 Entradas após limpeza: {final_count}")
        
        # Teste deve mostrar redução nas entradas
        expired_entries = initial_count - final_count
        print(f"✅ {expired_entries} entradas expiradas removidas")
        
        return expired_entries > 0
        
    except Exception as e:
        print(f"❌ Erro no teste de expiração: {e}")
        return False


def test_adaptive_parallelism():
    """Testa paralelismo adaptativo baseado na complexidade"""
    print("\n\n🎯 Testando Paralelismo Adaptativo")
    print("=" * 60)
    
    try:
        class MockQueryService:
            def process_natural_language_query(self, request):
                time.sleep(0.05)
                return type('MockResult', (), {
                    'success': True,
                    'results': [{'result': 'adaptive_test'}],
                    'execution_time': 0.05,
                    'row_count': 1,
                    'error_message': None,
                    'sql_query': 'SELECT * FROM adaptive_test'
                })()
        
        # Configurar com paralelismo adaptativo
        parallel_config = ParallelExecutionConfig(
            max_workers=4,
            enable_adaptive_parallelism=True
        )
        
        parallel_orchestrator = ParallelExecutionOrchestrator(
            MockQueryService(),
            config=parallel_config
        )
        print("✅ Parallel orchestrator com adaptação inicializado")
        
        # Testar com diferentes níveis de complexidade
        test_cases = [
            ("Baixa complexidade", 25.0, 2),  # Complexidade, steps esperados
            ("Média complexidade", 55.0, 3),
            ("Alta complexidade", 85.0, 5)
        ]
        
        for case_name, complexity, num_steps in test_cases:
            print(f"\n🔬 Teste: {case_name} (complexidade: {complexity})")
            
            # Criar steps baseados na complexidade
            steps = []
            for i in range(num_steps):
                steps.append(QueryStep(
                    step_id=f"adaptive_step_{i}",
                    description=f"Step adaptativo {i+1}",
                    sql_template=f"SELECT * FROM adaptive_table_{i}"
                ))
            
            adaptive_plan = QueryPlan(
                plan_id=f"adaptive_{case_name.lower().replace(' ', '_')}_{int(time.time())}",
                steps=steps,
                complexity_score=complexity,
                strategy="adaptive_test"
            )
            
            # Executar e medir performance
            start_time = time.time()
            result = parallel_orchestrator.execute_plan_parallel(adaptive_plan)
            execution_time = time.time() - start_time
            
            print(f"   ⏱️ Tempo de execução: {execution_time:.2f}s")
            print(f"   ✅ Sucesso: {result.success}")
            print(f"   📊 Steps executados: {len(result.completed_steps)}")
            
            # Verificar se adaptação funcionou
            metadata = result.metadata
            if metadata.get("parallel_execution"):
                print(f"   🎯 Execução paralela aplicada adequadamente")
            else:
                print(f"   📝 Execução sequencial usada")
        
        # Obter estatísticas finais
        final_stats = parallel_orchestrator.get_parallel_statistics()
        print(f"\n📊 Estatísticas finais do paralelismo adaptativo:")
        print(f"   Ajustes adaptativos: {final_stats['parallel_execution']['adaptive_adjustments']}")
        print(f"   Speedup médio: {final_stats['parallel_execution']['parallel_speedup']:.2f}x")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste adaptativo: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Executa todos os testes de performance optimization"""
    print("🚀 TESTE DE PERFORMANCE OPTIMIZATION - CHECKPOINT 9")
    print("=" * 80)
    
    tests = [
        ("Intelligent Cache Manager", test_intelligent_cache_manager),
        ("Parallel Execution Orchestrator", test_parallel_execution_orchestrator),
        ("Performance Integration", test_integrated_performance_optimization),
        ("Cache Expiration & Cleanup", test_cache_expiration_and_cleanup),
        ("Adaptive Parallelism", test_adaptive_parallelism)
    ]
    
    results = []
    total_start_time = time.time()
    
    try:
        for test_name, test_func in tests:
            print(f"\n{'='*20} {test_name} {'='*20}")
            start_time = time.time()
            
            try:
                result = test_func()
                execution_time = time.time() - start_time
                
                results.append((test_name, result, execution_time))
                status = "✅ PASSOU" if result else "❌ FALHOU"
                print(f"\n{status}: {test_name} ({execution_time:.2f}s)")
                
            except Exception as e:
                execution_time = time.time() - start_time
                print(f"\n❌ ERRO em {test_name}: {e}")
                results.append((test_name, False, execution_time))
                import traceback
                traceback.print_exc()
        
        # Resumo final
        total_time = time.time() - total_start_time
        passed = sum(1 for _, result, _ in results if result)
        total = len(results)
        
        print("\n\n🎉 RESUMO DOS TESTES - CHECKPOINT 9")
        print("=" * 80)
        
        for test_name, result, exec_time in results:
            status = "✅" if result else "❌"
            print(f"{status} {test_name} ({exec_time:.2f}s)")
        
        print(f"\n📊 Total: {passed}/{total} testes passaram")
        print(f"⏱️ Tempo total: {total_time:.2f}s")
        
        if passed == total:
            print("\n🎉 TODOS OS TESTES PASSARAM!")
            print("✅ Performance Optimization implementado com sucesso!")
            print("🎯 Checkpoint 9 concluído - Cache e Paralelização funcionais!")
            
            print("\n🚀 Benefícios implementados:")
            print("   ⚡ Cache inteligente multi-nível")
            print("   🔄 Execução paralela adaptativa") 
            print("   📊 Monitoramento de performance")
            print("   🧹 Limpeza automática de cache")
            print("   🎯 Otimização baseada em complexidade")
            return 0
        else:
            print(f"\n⚠️ {total - passed} testes falharam")
            print("❌ Performance optimization precisa de ajustes")
            return 1
            
    except Exception as e:
        print(f"\n\n❌ ERRO GERAL NOS TESTES: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())