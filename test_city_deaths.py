#!/usr/bin/env python3
"""Script para testar a pergunta sobre mortes por cidade"""

import sys
import os
sys.path.append('/home/maiconkevyn/PycharmProjects/txt2sql_claude_s')

from src.application.config.simple_config import ApplicationConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_city_deaths():
    """Testa a pergunta sobre mortes por cidade"""
    
    print("🚀 Testando: 'Quantas mortes ao total em cada cidade?'")
    
    # Configuração
    config = ApplicationConfig(
        llm_model="mistral",
        llm_provider="ollama"
    )
    
    # Inicializar orquestrador
    orchestrator = Text2SQLOrchestrator(config)
    
    test_query = "Quantas mortes ao total em cada cidade?"
    
    try:
        print(f"📝 Pergunta: {test_query}")
        print("="*60)
        
        # Primeiro executar SQL para ver a query gerada
        sql_result = orchestrator.process_single_query(test_query)
        print(f"🗄️ SQL gerada: {sql_result.sql_query}")
        print(f"📊 Sucesso: {sql_result.success}")
        print(f"📋 Primeiros resultados: {sql_result.results[:5] if sql_result.results else 'Nenhum'}")
        print(f"📏 Total de resultados: {len(sql_result.results) if sql_result.results else 0}")
        
        print("\n" + "="*60)
        
        # Depois executar versão conversacional
        result = orchestrator.process_conversational_query(test_query)
        print(f"💬 Resposta conversacional: {result['response']}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_city_deaths()