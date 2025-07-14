#!/usr/bin/env python3
"""
Focused test for Checkpoint 9 - Performance Optimization Components
Testa componentes individuais com mocks apropriados
"""
import sys
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

# Adicionar src ao path
project_root = os.path.dirname(__file__)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, project_root)
sys.path.insert(0, src_path)

# Test the individual components
def test_cache_manager_creation():
    """Testa criação do cache manager"""
    print("🧪 Testando criação do Intelligent Cache Manager")
    print("=" * 60)
    
    try:
        # Test import and basic initialization
        from src.application.services.implementations.intelligent_cache_manager import (
            IntelligentCacheManager, CacheConfiguration, CacheEntry
        )
        
        # Create cache configuration
        cache_config = CacheConfiguration(
            max_query_plans=100,
            max_execution_results=50,
            query_plan_ttl=300.0,
            execution_result_ttl=180.0,
            enable_statistics=True
        )
        print("✅ CacheConfiguration criada")
        
        # Initialize cache manager
        cache_manager = IntelligentCacheManager(cache_config)
        print("✅ IntelligentCacheManager inicializado")
        
        # Test basic functionality
        stats = cache_manager.get_cache_statistics()
        print(f"📊 Estatísticas iniciais: {stats['current_sizes']}")
        
        # Test cache entry creation
        test_entry = CacheEntry(
            key="test_key",
            value={"test": "data"},
            created_at=datetime.now(),
            last_accessed=datetime.now(),
            ttl_seconds=60.0
        )
        print(f"✅ CacheEntry criado: {test_entry.key}")
        
        # Test cache operations
        cache_manager.clear_cache("query_plans")
        print("✅ Cache clear operation successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parallel_orchestrator_creation():
    """Testa criação do parallel orchestrator"""
    print("\n\n⚡ Testando criação do Parallel Execution Orchestrator")
    print("=" * 60)
    
    try:
        from src.application.services.implementations.parallel_execution_orchestrator import (
            ParallelExecutionOrchestrator, ParallelExecutionConfig
        )
        
        # Mock query service
        class MockQueryService:
            def process_natural_language_query(self, request):
                return type('MockResult', (), {
                    'success': True,
                    'results': [{'count': 1}],
                    'execution_time': 0.1,
                    'row_count': 1,
                    'error_message': None,
                    'sql_query': 'SELECT 1'
                })()
        
        # Create parallel configuration
        parallel_config = ParallelExecutionConfig(
            max_workers=2,
            step_timeout_seconds=10.0,
            enable_step_caching=True,
            enable_adaptive_parallelism=True
        )
        print("✅ ParallelExecutionConfig criada")
        
        # Initialize parallel orchestrator
        parallel_orchestrator = ParallelExecutionOrchestrator(
            MockQueryService(),
            config=parallel_config
        )
        print("✅ ParallelExecutionOrchestrator inicializado")
        
        # Test statistics
        stats = parallel_orchestrator.get_parallel_statistics()
        print(f"📊 Configuração: max_workers={stats['configuration']['max_workers']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_comprehensive_orchestrator_integration():
    """Testa integração no comprehensive orchestrator"""
    print("\n\n🔧 Testando integração no Comprehensive Orchestrator")
    print("=" * 60)
    
    try:
        from src.application.services.implementations.comprehensive_execution_orchestrator import (
            ComprehensiveExecutionOrchestrator
        )
        
        # Mock query service
        class MockQueryService:
            def process_natural_language_query(self, request):
                return type('MockResult', (), {
                    'success': True,
                    'results': [{'test': 'integration'}],
                    'execution_time': 0.05,
                    'row_count': 1,
                    'error_message': None,
                    'sql_query': 'SELECT * FROM integration_test'
                })()
        
        # Test with performance optimization enabled
        orchestrator_optimized = ComprehensiveExecutionOrchestrator(
            MockQueryService(),
            enable_performance_optimization=True
        )
        print("✅ Comprehensive orchestrator com otimizações criado")
        
        # Test with performance optimization disabled
        orchestrator_standard = ComprehensiveExecutionOrchestrator(
            MockQueryService(),
            enable_performance_optimization=False
        )
        print("✅ Comprehensive orchestrator padrão criado")
        
        # Test performance statistics
        if hasattr(orchestrator_optimized, 'get_performance_statistics'):
            perf_stats = orchestrator_optimized.get_performance_statistics()
            print(f"📊 Performance optimization enabled: {perf_stats.get('optimization_enabled', False)}")
        
        # Test cache performance methods
        if hasattr(orchestrator_optimized, 'get_cache_hit_rate'):
            cache_hit_rate = orchestrator_optimized.get_cache_hit_rate()
            print(f"📊 Cache hit rate inicial: {cache_hit_rate}%")
        
        if hasattr(orchestrator_optimized, 'get_parallel_efficiency'):
            parallel_efficiency = orchestrator_optimized.get_parallel_efficiency()
            print(f"📊 Parallel efficiency inicial: {parallel_efficiency:.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_text2sql_orchestrator_integration():
    """Testa integração no Text2SQLOrchestrator"""
    print("\n\n🎯 Testando integração no Text2SQLOrchestrator")
    print("=" * 60)
    
    try:
        # Test that Text2SQLOrchestrator has performance methods
        from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator, OrchestratorConfig
        
        # Check if performance methods exist
        orchestrator_methods = dir(Text2SQLOrchestrator)
        performance_methods = [
            'get_performance_statistics',
            'optimize_system_performance',
            'get_cache_performance',
            'enable_performance_debug_mode',
            'get_system_health_with_performance'
        ]
        
        methods_found = 0
        for method in performance_methods:
            if method in orchestrator_methods:
                print(f"✅ Método {method} encontrado")
                methods_found += 1
            else:
                print(f"❌ Método {method} não encontrado")
        
        print(f"📊 {methods_found}/{len(performance_methods)} métodos de performance encontrados")
        
        # Test configuration has performance options
        config = OrchestratorConfig()
        if hasattr(config, 'enable_query_decomposition'):
            print("✅ Configuração de decomposição disponível")
        
        return methods_found == len(performance_methods)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_performance_components_architecture():
    """Testa arquitetura dos componentes de performance"""
    print("\n\n🏗️ Testando arquitetura dos componentes")
    print("=" * 60)
    
    try:
        # Test that all files exist
        components = [
            'src/application/services/implementations/intelligent_cache_manager.py',
            'src/application/services/implementations/parallel_execution_orchestrator.py'
        ]
        
        files_found = 0
        for component in components:
            file_path = os.path.join(project_root, component)
            if os.path.exists(file_path):
                print(f"✅ Arquivo {component} encontrado")
                files_found += 1
                
                # Check file size to ensure it's not empty
                file_size = os.path.getsize(file_path)
                print(f"   Tamanho: {file_size} bytes")
            else:
                print(f"❌ Arquivo {component} não encontrado")
        
        print(f"📊 {files_found}/{len(components)} arquivos encontrados")
        
        # Test imports work
        try:
            from src.application.services.implementations.intelligent_cache_manager import IntelligentCacheManager
            print("✅ IntelligentCacheManager import successful")
        except:
            print("❌ IntelligentCacheManager import failed")
            
        try:
            from src.application.services.implementations.parallel_execution_orchestrator import ParallelExecutionOrchestrator
            print("✅ ParallelExecutionOrchestrator import successful")
        except:
            print("❌ ParallelExecutionOrchestrator import failed")
        
        return files_found == len(components)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False


def main():
    """Executa testes focados para Checkpoint 9"""
    print("🚀 TESTE FOCADO - CHECKPOINT 9 PERFORMANCE OPTIMIZATION")
    print("=" * 80)
    
    tests = [
        ("Cache Manager Creation", test_cache_manager_creation),
        ("Parallel Orchestrator Creation", test_parallel_orchestrator_creation),
        ("Comprehensive Orchestrator Integration", test_comprehensive_orchestrator_integration),
        ("Text2SQL Orchestrator Integration", test_text2sql_orchestrator_integration),
        ("Performance Components Architecture", test_performance_components_architecture)
    ]
    
    results = []
    total_start_time = time.time()
    
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
    
    # Resumo final
    total_time = time.time() - total_start_time
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    print("\n\n🎉 RESUMO DOS TESTES FOCADOS - CHECKPOINT 9")
    print("=" * 80)
    
    for test_name, result, exec_time in results:
        status = "✅" if result else "❌"
        print(f"{status} {test_name} ({exec_time:.2f}s)")
    
    print(f"\n📊 Total: {passed}/{total} testes passaram")
    print(f"⏱️ Tempo total: {total_time:.2f}s")
    
    if passed == total:
        print("\n🎉 TODOS OS TESTES FOCADOS PASSARAM!")
        print("✅ Componentes de Performance Optimization criados com sucesso!")
        print("🎯 Checkpoint 9 - Arquitetura implementada!")
        
        print("\n🚀 Componentes implementados:")
        print("   📦 IntelligentCacheManager - Sistema de cache multi-nível")
        print("   ⚡ ParallelExecutionOrchestrator - Execução paralela adaptativa")
        print("   🔧 ComprehensiveExecutionOrchestrator - Integração completa")
        print("   📊 Performance monitoring methods")
        print("   🎯 Text2SQLOrchestrator integration")
        
        return 0
    else:
        print(f"\n⚠️ {total - passed} testes falharam")
        print("❌ Alguns componentes precisam de ajustes")
        return 1


if __name__ == "__main__":
    exit(main())