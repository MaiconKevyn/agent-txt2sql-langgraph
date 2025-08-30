#!/usr/bin/env python3
"""
Teste de queries básicas com PostgreSQL SIH-RS
Validação do agente LangGraph V3 com nova base
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.langgraph_migration.orchestrator_v3 import LangGraphOrchestrator

def test_basic_queries():
    """Testa queries básicas com PostgreSQL SIH-RS"""
    
    print("=== TESTE QUERIES BÁSICAS POSTGRESQL SIH-RS ===\n")
    
    # Configurar para PostgreSQL
    config = ApplicationConfig()
    orchestrator_config = OrchestratorConfig()
    
    print(f"Database: {config.database_type}")
    print(f"URI: {config.database_path}")
    
    try:
        # Inicializar LangGraph Orchestrator
        print("\n1. Inicializando LangGraph Orchestrator...")
        orchestrator = LangGraphOrchestrator(config, orchestrator_config)
        print("✅ Orchestrator inicializado com sucesso!")
        
        # Queries de teste
        test_queries = [
            "Quantas internações existem no total?",
            "Qual cidade com mais internações?", 
            "Quantos homens e mulheres foram internados?",
            "Quais os 5 diagnósticos mais comuns?",
            "O que significa o código CID F190?"
        ]
        
        print(f"\n2. Testando {len(test_queries)} queries...")
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n--- QUERY {i}: {query} ---")
            
            try:
                # Executar query
                result = orchestrator.process_query(query)
                
                # Extrair resposta
                if isinstance(result, dict):
                    answer = result.get('final_answer', result.get('answer', 'Sem resposta'))
                    execution_time = result.get('execution_time', 'N/A')
                    query_type = result.get('query_type', 'N/A')
                    
                    print(f"✅ Tipo: {query_type}")
                    print(f"✅ Tempo: {execution_time}s")
                    print(f"✅ Resposta: {answer[:200]}...")
                    
                else:
                    print(f"✅ Resposta: {str(result)[:200]}...")
                    
            except Exception as e:
                print(f"❌ Erro na query {i}: {e}")
                continue
        
        print("\n🎉 TESTE DE QUERIES CONCLUÍDO!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_basic_queries()
    sys.exit(0 if success else 1)