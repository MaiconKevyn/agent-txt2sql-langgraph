#!/usr/bin/env python3
"""
Teste do Orquestrador LangGraph com pergunta específica
"""

import time
from src.langgraph_migration.pure_compatibility_wrapper import PureLangGraphWrapper

def test_langgraph_orchestrator():
    print("🚀 Iniciando Orquestrador LangGraph...")
    
    # Inicializar o wrapper LangGraph
    wrapper = PureLangGraphWrapper()
    
    print("✅ Orquestrador inicializado com sucesso!")
    print("📋 Configuração:")
    print("   - Workflow: 4 nodes (classify → sql → response → format)")
    print("   - Models: llama3.1:8b (SQL), mistral:latest (conversational)")
    print("   - Database: sus_database.db (58,655 registros)")
    
    # Testar com a pergunta específica
    test_query = "Em qual cidade morrem mais homens?"
    print(f"\n🧪 Testando pergunta: \"{test_query}\"")
    
    start_time = time.time()
    try:
        result = wrapper.process_single_query(test_query)
        execution_time = time.time() - start_time
        
        print("\n📊 RESULTADOS:")
        print(f"   ✅ Sucesso: {result.success}")
        print(f"   ⏱️  Tempo de execução: {execution_time:.2f}s")
        
        if result.sql_query and result.sql_query.strip():
            print(f"   🗄️  SQL Gerado: {result.sql_query.strip()}")
        
        if result.results and len(result.results) > 0:
            print(f"   📈 Registros retornados: {len(result.results)}")
            # Mostrar primeiros resultados
            for i, row in enumerate(result.results[:5]):
                print(f"      {i+1}. {row}")
            if len(result.results) > 5:
                print(f"      ... e mais {len(result.results) - 5} registros")
        
        if result.final_response:
            if result.final_response != "Resposta não disponível":
                print(f"   💬 Resposta: {result.final_response}")
            else:
                print("   ⚠️  Resposta conversacional não gerada")
        
        if result.error_message:
            print(f"   ❌ Erro: {result.error_message}")
            
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"   💥 Exceção após {execution_time:.2f}s: {e}")
        import traceback
        traceback.print_exc()
    
    # Estatísticas da sessão
    print("\n📈 Estatísticas da Sessão:")
    stats = wrapper.get_session_info()
    print(f"   Total de consultas: {stats['query_count']}")
    print(f"   Taxa de sucesso: {wrapper._get_success_rate():.1f}%")
    print(f"   Tempo médio: {wrapper._stats['average_response_time']:.2f}s")
    
    # Health check final
    print("\n🏥 Health Check:")
    health = wrapper.health_check()
    print(f"   Status: {health['status']}")
    print(f"   Versão: {health['system'].get('version', 'unknown')}")
    
    return result

if __name__ == "__main__":
    test_langgraph_orchestrator()