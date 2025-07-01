#!/usr/bin/env python3
"""
Test script for query routing integration
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.application.container.dependency_injection import (
    ContainerFactory, 
    ServiceConfig
)
from src.application.orchestrator.text2sql_orchestrator import (
    Text2SQLOrchestrator,
    OrchestratorConfig
)
from src.application.services.query_classification_service import (
    IQueryClassificationService,
    QueryType
)


def test_classification_service():
    """Test query classification service"""
    print("🧪 Testing Query Classification Service...")
    
    try:
        # Create container with classification enabled
        config = ServiceConfig(
            database_path="sus_database.db",
            enable_query_classification=True,
            query_classification_confidence_threshold=0.7
        )
        container = ContainerFactory.create_container_with_config(config)
        container.initialize()
        
        # Get classification service
        classification_service = container.get_service(IQueryClassificationService)
        
        # Test cases
        test_queries = [
            ("Quantos pacientes existem no banco?", QueryType.DATABASE_QUERY),
            ("O que significa CID J90?", QueryType.CONVERSATIONAL_QUERY),
            ("Qual a média de idade dos pacientes?", QueryType.DATABASE_QUERY),
            ("Explique o que é hipertensão", QueryType.CONVERSATIONAL_QUERY),
            ("Quantas mortes em Porto Alegre?", QueryType.DATABASE_QUERY),
            ("Para que serve o SUS?", QueryType.CONVERSATIONAL_QUERY)
        ]
        
        results = []
        for query, expected_type in test_queries:
            classification = classification_service.classify_query(query)
            
            success = classification.query_type == expected_type
            confidence = classification.confidence_score
            
            print(f"  📝 Query: {query[:40]}...")
            print(f"     ✅ Esperado: {expected_type.value}")
            print(f"     🎯 Detectado: {classification.query_type.value}")
            print(f"     📊 Confiança: {confidence:.2f}")
            print(f"     ✅ Sucesso: {'✓' if success else '✗'}")
            print(f"     💭 Raciocínio: {classification.reasoning}")
            print()
            
            results.append({
                'query': query,
                'expected': expected_type,
                'detected': classification.query_type,
                'confidence': confidence,
                'success': success,
                'reasoning': classification.reasoning
            })
        
        # Summary
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r['success'])
        accuracy = successful_tests / total_tests
        
        print(f"📊 Resumo dos Testes de Classificação:")
        print(f"   Total: {total_tests}")
        print(f"   Sucessos: {successful_tests}")
        print(f"   Acurácia: {accuracy:.2%}")
        print()
        
        return accuracy >= 0.7  # 70% accuracy threshold
        
    except Exception as e:
        print(f"❌ Erro no teste de classificação: {e}")
        return False


def test_orchestrator_routing():
    """Test orchestrator routing functionality"""
    print("🧪 Testing Orchestrator Routing...")
    
    try:
        # Create orchestrator with routing enabled
        service_config = ServiceConfig(
            database_path="sus_database.db",
            enable_query_classification=True,
            llm_model="llama3"
        )
        
        orchestrator_config = OrchestratorConfig(
            enable_query_routing=True,
            routing_confidence_threshold=0.7
        )
        
        container = ContainerFactory.create_container_with_config(service_config)
        orchestrator = Text2SQLOrchestrator(container, orchestrator_config)
        
        # Test routing with different query types
        test_cases = [
            {
                "query": "O que significa CID J90?",
                "expected_route": "conversational",
                "description": "Pergunta explicativa sobre CID"
            },
            {
                "query": "Quantos pacientes existem?",
                "expected_route": "database",
                "description": "Query de contagem para banco"
            }
        ]
        
        routing_results = []
        for test_case in test_cases:
            query = test_case["query"]
            expected = test_case["expected_route"]
            description = test_case["description"]
            
            print(f"  📝 Teste: {description}")
            print(f"     Query: {query}")
            print(f"     Rota esperada: {expected}")
            
            # Process query
            result = orchestrator.process_single_query(query)
            
            # Check metadata for routing information
            routed_correctly = False
            actual_route = "unknown"
            
            if result.metadata:
                query_type = result.metadata.get("query_classification")
                routing_method = result.metadata.get("routing_method")
                
                if query_type == "conversational_query":
                    actual_route = "conversational"
                elif query_type == "database_query":
                    actual_route = "database"
                    
                routed_correctly = (
                    (expected == "conversational" and actual_route == "conversational") or
                    (expected == "database" and actual_route == "database")
                )
            
            print(f"     Rota detectada: {actual_route}")
            print(f"     Sucesso: {'✓' if routed_correctly else '✗'}")
            print(f"     Resultado: {'✓' if result.success else '✗'}")
            print()
            
            routing_results.append({
                'query': query,
                'expected': expected,
                'actual': actual_route,
                'success': routed_correctly,
                'result_success': result.success
            })
        
        # Summary
        total_routing_tests = len(routing_results)
        successful_routing = sum(1 for r in routing_results if r['success'])
        routing_accuracy = successful_routing / total_routing_tests
        
        print(f"📊 Resumo dos Testes de Roteamento:")
        print(f"   Total: {total_routing_tests}")
        print(f"   Sucessos: {successful_routing}")
        print(f"   Acurácia: {routing_accuracy:.2%}")
        print()
        
        return routing_accuracy >= 0.5  # 50% accuracy threshold (more lenient)
        
    except Exception as e:
        print(f"❌ Erro no teste de roteamento: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_container_health():
    """Test that all services are healthy"""
    print("🧪 Testing Container Health...")
    
    try:
        config = ServiceConfig(
            database_path="sus_database.db",
            enable_query_classification=True
        )
        container = ContainerFactory.create_container_with_config(config)
        container.initialize()
        
        health_status = container.health_check()
        
        print(f"  📊 Status geral: {health_status['status']}")
        
        all_healthy = True
        for service_name, service_health in health_status['services'].items():
            is_healthy = service_health.get('healthy', False)
            status_icon = "✅" if is_healthy else "❌"
            print(f"  {status_icon} {service_name}: {'OK' if is_healthy else 'ERRO'}")
            
            if not is_healthy:
                all_healthy = False
        
        return all_healthy
        
    except Exception as e:
        print(f"❌ Erro no teste de saúde: {e}")
        return False


def main():
    """Run all tests"""
    print("🚀 Iniciando Testes de Integração do Roteamento de Queries")
    print("=" * 60)
    print()
    
    tests = [
        ("Container Health Check", test_container_health),
        ("Query Classification", test_classification_service),
        ("Orchestrator Routing", test_orchestrator_routing)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"▶️ Executando: {test_name}")
        print("-" * 40)
        
        try:
            success = test_func()
            results.append((test_name, success))
            
            if success:
                print(f"✅ {test_name}: PASSOU")
            else:
                print(f"❌ {test_name}: FALHOU")
        except Exception as e:
            print(f"💥 {test_name}: ERRO - {e}")
            results.append((test_name, False))
        
        print()
    
    # Final summary
    print("=" * 60)
    print("📊 RESUMO FINAL DOS TESTES")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    
    for test_name, success in results:
        status = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"  {status} - {test_name}")
    
    print()
    print(f"Total: {total_tests}")
    print(f"Passou: {passed_tests}")
    print(f"Falhou: {total_tests - passed_tests}")
    print(f"Taxa de sucesso: {passed_tests/total_tests:.1%}")
    
    if passed_tests == total_tests:
        print("\n🎉 TODOS OS TESTES PASSARAM! Sistema de roteamento funcionando!")
        return 0
    else:
        print(f"\n⚠️ {total_tests - passed_tests} teste(s) falharam. Verifique a implementação.")
        return 1


if __name__ == "__main__":
    sys.exit(main())