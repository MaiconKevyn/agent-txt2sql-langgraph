#!/usr/bin/env python3
"""
DEBUG: Investigar por que SQL não está sendo gerado
"""

import sys
sys.path.append('..')

def debug_sql_generation():
    print("🐛 DEBUG: Investigando geração de SQL")
    print("=" * 50)
    
    try:
        from src.langgraph_migration.orchestrator_v3 import create_production_orchestrator
        
        # Criar orchestrador
        orchestrator = create_production_orchestrator(
            provider="ollama",
            model_name="llama3.1:8b"
        )
        
        # Query simples
        query = "Quantos pacientes existem?"
        print(f"🧪 Query: {query}")
        
        # Executar com debug
        result = orchestrator.process_query(query)
        
        print(f"\n📊 RESULTADO COMPLETO:")
        print(f"Success: {result.get('success')}")
        print(f"SQL Query: {result.get('sql_query')}")
        print(f"Response: {result.get('response')}")
        print(f"Error: {result.get('error_message')}")
        print(f"Route: {result.get('route_used')}")
        print(f"Row Count: {result.get('row_count')}")
        
        # Verificar todas as chaves
        print(f"\n🔍 TODAS AS CHAVES NO RESULTADO:")
        for key, value in result.items():
            print(f"  {key}: {type(value)} = {value}")
        
        # Tentar query mais específica
        print(f"\n🧪 Testando query mais específica...")
        query2 = "SELECT COUNT(*) FROM pacientes"
        result2 = orchestrator.process_query(query2)
        
        print(f"Success: {result2.get('success')}")
        print(f"SQL Query: {result2.get('sql_query')}")
        print(f"Response: {result2.get('response')}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

def debug_llm_manager():
    print("\n🔧 DEBUG: LLM Manager")
    print("=" * 30)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        print("✅ LLM Manager criado")
        
        # Testar SQL generation diretamente
        schema_context = "tabela: pacientes, colunas: id, nome, idade"
        result = llm_manager.generate_sql_query(
            user_query="Quantos pacientes existem?",
            schema_context=schema_context
        )
        
        print(f"📊 Resultado do LLM Manager:")
        print(f"Success: {result.get('success')}")
        print(f"SQL: {result.get('sql_query')}")
        print(f"Error: {result.get('error')}")
        
    except Exception as e:
        print(f"❌ Erro no LLM Manager: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_sql_generation()
    debug_llm_manager()