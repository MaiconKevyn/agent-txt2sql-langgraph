#!/usr/bin/env python3
"""Script para testar o prompt melhorado com diferentes tipos de consulta"""

import sys
import os
sys.path.append('/home/maiconkevyn/PycharmProjects/txt2sql_claude_s')

from src.application.config.simple_config import ApplicationConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_advanced_prompt():
    """Testa o prompt melhorado com diferentes tipos de consulta"""
    
    print("🚀 Testando prompt melhorado com técnicas avançadas...")
    
    # Configuração
    config = ApplicationConfig(
        llm_model="mistral",
        llm_provider="ollama"
    )
    
    # Inicializar orquestrador
    orchestrator = Text2SQLOrchestrator(config)
    
    # Testes com diferentes tipos de consulta
    test_queries = [
        "Quantas pessoas morreram?",  # CONTAGEM_SIMPLES
        "Qual cidade tem mais casos?",  # IDENTIFICAÇÃO_GEOGRÁFICA  
        "Quantos casos por município?",  # CONTAGEM_GEOGRÁFICA
    ]
    
    for i, query in enumerate(test_queries):
        print(f"\n{'='*60}")
        print(f"TESTE {i+1}: {query}")
        print('='*60)
        
        try:
            result = orchestrator.process_conversational_query(query)
            print(f"✅ Resposta: {result['response']}")
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_advanced_prompt()