#!/usr/bin/env python3
"""Script para testar os logs melhorados do sistema conversacional"""

import sys
import os
sys.path.append('/')

from src.application.config.simple_config import ApplicationConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_enhanced_logging():
    """Testa os logs melhorados com uma query simples"""
    
    print("🚀 Iniciando teste de logs melhorados...")
    
    # Configuração
    config = ApplicationConfig(
        llm_model="mistral",
        llm_provider="ollama"
    )
    
    # Inicializar orquestrador
    orchestrator = Text2SQLOrchestrator(config)
    
    # Fazer query teste
    test_query = "Quantas pessoas morreram?"
    print(f"📝 Testando query: {test_query}")
    
    try:
        # Primeiro, fazer um process_single_query para obter resultado SQL
        print("🔍 Primeiro, executando process_single_query...")
        sql_result = orchestrator.process_single_query(test_query)
        print(f"📊 SQL Result: {sql_result.success}, {len(sql_result.results) if sql_result.results else 0} rows")
        
        # Depois, fazer process_conversational_query 
        print("🔄 Agora, executando process_conversational_query...")
        result = orchestrator.process_conversational_query(test_query)
        print(f"✅ Resultado conversacional: {result['response']}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_logging()