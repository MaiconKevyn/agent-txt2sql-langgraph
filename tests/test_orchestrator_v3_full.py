#!/usr/bin/env python3
"""
Teste Completo do Orquestrador LangGraph V3
Teste com a pergunta específica: "Em qual cidade morrem mais homens?"
"""

import sys
import os
import time
from datetime import datetime

# Add src to path
sys.path.append('../src')

try:
    from src.langgraph_migration.orchestrator_v3 import create_production_orchestrator
    print("✅ Import do Orquestrador V3: SUCCESS")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_orchestrator_v3():
    print("🚀 TESTE COMPLETO DO ORQUESTRADOR LANGGRAPH V3")
    print("=" * 60)
    
    # Inicializar orquestrador
    print("\n📋 1. INICIALIZANDO ORQUESTRADOR V3...")
    try:
        orchestrator = create_production_orchestrator(
            provider="ollama", 
            model_name="llama3.1:8b"
        )
        print("✅ Orquestrador V3 inicializado com sucesso!")
        
        # Health check
        health = orchestrator.health_check()
        print(f"✅ Health Check: {health['status']}")
        print(f"   Modelo atual: {health['current_model']['model_name']}")
        print(f"   Provider: {health['current_model']['provider']}")
        
    except Exception as e:
        print(f"❌ Falha na inicialização: {e}")
        return False
    
    # Teste com pergunta específica
    print("\n🧪 2. TESTANDO PERGUNTA ESPECÍFICA...")
    test_query = "Em qual cidade morrem mais homens?"
    print(f"   Pergunta: \"{test_query}\"")
    
    start_time = time.time()
    try:
        result = orchestrator.process_query(test_query)
        execution_time = time.time() - start_time
        
        print("\n📊 RESULTADOS:")
        print(f"   ✅ Sucesso: {result.get('success', False)}")
        print(f"   ⏱️  Tempo de execução: {execution_time:.2f}s")
        
        if result.get('sql_query'):
            print(f"   🗄️  SQL Gerado: {result['sql_query']}")
        
        if result.get('results') and len(result['results']) > 0:
            print(f"   📈 Registros retornados: {len(result['results'])}")
            print("   🎯 Top 5 resultados:")
            for i, row in enumerate(result['results'][:5]):
                print(f"      {i+1}. {row}")
        
        if result.get('response'):
            print(f"   💬 Resposta: {result['response']}")
        
        if result.get('error_message'):
            print(f"   ❌ Erro: {result['error_message']}")
        
        # Metadata do workflow
        metadata = result.get('metadata', {})
        if metadata:
            print(f"   🔧 Orquestrador V3: {metadata.get('orchestrator_v3', False)}")
            if 'current_model' in metadata:
                model_info = metadata['current_model']
                print(f"   🧠 Modelo usado: {model_info.get('model_name')} ({model_info.get('provider')})")
            print(f"   🔢 Query #: {metadata.get('query_number', 'N/A')}")
        
        return result.get('success', False)
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"   💥 Exceção após {execution_time:.2f}s: {e}")
        return False

def test_multiple_query_types():
    """Teste com diferentes tipos de queries"""
    print("\n🎯 3. TESTANDO DIFERENTES TIPOS DE QUERIES...")
    
    orchestrator = create_production_orchestrator(provider="ollama", model_name="llama3.1:8b")
    
    test_queries = [
        ("Database Query", "Quantos pacientes existem no total?"),
        ("Conversational Query", "O que significa CID J90?"),
        ("Complex Query", "Qual a média de idade dos pacientes por cidade?")
    ]
    
    results = []
    for query_type, query in test_queries:
        print(f"\n   🧪 {query_type}: \"{query}\"")
        start_time = time.time()
        
        try:
            result = orchestrator.process_query(query)
            execution_time = time.time() - start_time
            
            print(f"      ✅ Sucesso: {result.get('success', False)}")
            print(f"      ⏱️  Tempo: {execution_time:.2f}s")
            
            if result.get('sql_query'):
                print(f"      🗄️  SQL: {result['sql_query'][:50]}...")
            
            results.append({
                "type": query_type,
                "success": result.get('success', False),
                "time": execution_time
            })
            
        except Exception as e:
            print(f"      ❌ Erro: {e}")
            results.append({
                "type": query_type,
                "success": False,
                "time": time.time() - start_time
            })
    
    return results

def main():
    start_time = datetime.now()
    
    print(f"🕒 Iniciado em: {start_time}")
    
    # Teste principal
    main_success = test_orchestrator_v3()
    
    # Testes adicionais
    additional_results = test_multiple_query_types()
    
    # Resumo final
    print("\n" + "=" * 60)
    print("📋 RESUMO DOS TESTES:")
    print(f"   Teste principal: {'✅ PASSOU' if main_success else '❌ FALHOU'}")
    
    success_count = sum(1 for r in additional_results if r['success'])
    total_tests = len(additional_results)
    print(f"   Testes adicionais: {success_count}/{total_tests} sucessos")
    
    avg_time = sum(r['time'] for r in additional_results) / len(additional_results) if additional_results else 0
    print(f"   Tempo médio: {avg_time:.2f}s")
    
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    print(f"   Duração total: {total_duration:.2f}s")
    
    overall_success = main_success and success_count == total_tests
    
    print(f"\n🏆 RESULTADO FINAL: {'✅ TODOS OS TESTES PASSARAM' if overall_success else '❌ ALGUNS TESTES FALHARAM'}")
    
    if overall_success:
        print("🚀 ORQUESTRADOR V3 ESTÁ PRONTO PARA PRODUÇÃO!")
    else:
        print("🔧 Revise os erros acima antes de colocar em produção")
    
    return overall_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)