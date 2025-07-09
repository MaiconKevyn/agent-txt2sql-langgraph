#!/usr/bin/env python3
"""
Test script to compare Direct LLM vs LangChain Agent methods
"""
import sys
import os
import time
import sqlite3

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from application.container.dependency_injection import DependencyContainer, ServiceConfig
from application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator, OrchestratorConfig

def test_query_with_both_methods(query_text: str):
    """Test query with both Direct LLM and LangChain Agent methods"""
    
    print(f"🔍 TESTANDO QUERY: '{query_text}'")
    print("=" * 80)
    
    # Initialize services
    service_config = ServiceConfig()
    
    # Test 1: Direct LLM (current default)
    print("\n1️⃣ TESTE COM DIRECT LLM (MÉTODO PADRÃO)")
    print("-" * 50)
    
    try:
        container = DependencyContainer(service_config)
        orchestrator_config = OrchestratorConfig(enable_conversational_responses=False)
        orchestrator = Text2SQLOrchestrator(container, orchestrator_config)
        
        start_time = time.time()
        result1 = orchestrator.process_single_query(query_text)
        end_time = time.time()
        
        print(f"✅ Sucesso: {result1.success}")
        print(f"⏱️ Tempo: {end_time - start_time:.2f}s")
        print(f"🔧 SQL: {result1.sql_query}")
        print(f"📊 Registros: {result1.row_count}")
        if result1.results and len(result1.results) > 0:
            print(f"🎯 Primeiro resultado: {result1.results[0]}")
        if result1.error_message:
            print(f"❌ Erro: {result1.error_message}")
            
        # Get validation metadata if available
        if result1.metadata and 'validation_score' in result1.metadata:
            print(f"🔍 Score de validação: {result1.metadata['validation_score']:.1f}/100")
        
    except Exception as e:
        print(f"❌ Erro no método Direct LLM: {str(e)}")
        result1 = None
    
    # Test 2: Force LangChain Agent (modify the service to use langchain primary)
    print("\n2️⃣ TESTE COM LANGCHAIN AGENT (FALLBACK)")
    print("-" * 50)
    
    try:
        # Create new container and force langchain primary
        container2 = DependencyContainer(service_config)
        container2.initialize()
        
        # Get the query processing service and force it to use langchain as primary
        query_service = container2.get_service('IQueryProcessingService')
        query_service._use_langchain_primary = True  # Force langchain as primary
        
        orchestrator_config2 = OrchestratorConfig(enable_conversational_responses=False)
        orchestrator2 = Text2SQLOrchestrator(container2, orchestrator_config2)
        
        start_time = time.time()
        result2 = orchestrator2.process_single_query(query_text)
        end_time = time.time()
        
        print(f"✅ Sucesso: {result2.success}")
        print(f"⏱️ Tempo: {end_time - start_time:.2f}s")
        print(f"🔧 SQL: {result2.sql_query}")
        print(f"📊 Registros: {result2.row_count}")
        if result2.results and len(result2.results) > 0:
            print(f"🎯 Primeiro resultado: {result2.results[0]}")
        if result2.error_message:
            print(f"❌ Erro: {result2.error_message}")
            
        # Check if it used langchain
        if result2.metadata and 'langchain_agent' in result2.metadata:
            print(f"🤖 Método usado: LangChain Agent")
        
    except Exception as e:
        print(f"❌ Erro no método LangChain Agent: {str(e)}")
        result2 = None
    
    # Comparison
    print("\n📊 COMPARAÇÃO DOS RESULTADOS")
    print("-" * 50)
    
    if result1 and result2:
        print(f"Direct LLM SQL: {result1.sql_query}")
        print(f"LangChain SQL:  {result2.sql_query}")
        print(f"SQLs são iguais: {'✅ SIM' if result1.sql_query == result2.sql_query else '❌ NÃO'}")
        
        if result1.results and result2.results and len(result1.results) > 0 and len(result2.results) > 0:
            print(f"Direct LLM resultado: {result1.results[0]}")
            print(f"LangChain resultado:  {result2.results[0]}")
            print(f"Resultados são iguais: {'✅ SIM' if result1.results[0] == result2.results[0] else '❌ NÃO'}")
    
    # Validate correct answer
    print("\n🎯 VALIDAÇÃO DA RESPOSTA CORRETA")
    print("-" * 50)
    
    try:
        conn = sqlite3.connect('../sus_database.db')
        cursor = conn.cursor()
        
        # Execute the correct query
        correct_query = """
        SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_mortes 
        FROM sus_data 
        WHERE IDADE < 30 AND MORTE = 1 
        GROUP BY CIDADE_RESIDENCIA_PACIENTE 
        ORDER BY total_mortes DESC 
        LIMIT 1;
        """
        
        cursor.execute(correct_query)
        correct_result = cursor.fetchone()
        
        if correct_result:
            cidade_correta, mortes_corretas = correct_result
            print(f"✅ RESPOSTA CORRETA: {cidade_correta} com {mortes_corretas} mortes")
            
            # Check if either method got it right
            if result1 and result1.results and len(result1.results) > 0:
                first_result1 = result1.results[0]
                if isinstance(first_result1, dict) and 'CIDADE_RESIDENCIA_PACIENTE' in first_result1:
                    if first_result1['CIDADE_RESIDENCIA_PACIENTE'] == cidade_correta:
                        print("✅ Direct LLM acertou!")
                    else:
                        print(f"❌ Direct LLM errou: {first_result1['CIDADE_RESIDENCIA_PACIENTE']}")
            
            if result2 and result2.results and len(result2.results) > 0:
                first_result2 = result2.results[0]
                if isinstance(first_result2, dict) and 'CIDADE_RESIDENCIA_PACIENTE' in first_result2:
                    if first_result2['CIDADE_RESIDENCIA_PACIENTE'] == cidade_correta:
                        print("✅ LangChain Agent acertou!")
                    else:
                        print(f"❌ LangChain Agent errou: {first_result2['CIDADE_RESIDENCIA_PACIENTE']}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro na validação: {str(e)}")


if __name__ == "__main__":
    query = "Qual é a cidade onde mais morrem pessoas com menos de 30 anos?"
    test_query_with_both_methods(query)