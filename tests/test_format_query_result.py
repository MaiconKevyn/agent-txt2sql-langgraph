#!/usr/bin/env python3
"""Script para testar especificamente o _format_query_result"""

import sys
import os
sys.path.append('/')

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator
from src.application.services.query_processing_service import QueryResult

def test_format_query_result():
    """Testa especificamente o _format_query_result"""
    
    print("🚀 Testando _format_query_result diretamente...")
    
    # Configuração
    config = ApplicationConfig(llm_model="mistral", llm_provider="ollama")
    orch_config = OrchestratorConfig()
    
    # Inicializar orquestrador
    orchestrator = Text2SQLOrchestrator(config, orch_config)
    
    # Primeiro executar uma query para obter resultado real
    test_query = "Quantas pessoas morreram?"
    print(f"📝 Executando query: {test_query}")
    
    sql_result = orchestrator.process_single_query(test_query)
    print(f"📊 SQL executada: {sql_result.sql_query}")
    print(f"📋 Resultados: {sql_result.results}")
    
    # Agora testar o _format_query_result diretamente
    print("🔄 Chamando _format_query_result diretamente...")
    formatted_response = orchestrator._format_query_result(sql_result, test_query)
    
    print(f"✅ Resposta formatada: {formatted_response.content}")
    print(f"📊 Metadados: {formatted_response.metadata}")

if __name__ == "__main__":
    test_format_query_result()