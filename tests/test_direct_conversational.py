#!/usr/bin/env python3
"""Script para testar diretamente o conversational service"""

import sys
import os
import logging
sys.path.append('/')

# Configure logging to see all levels
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')

from src.application.services.conversational_response_service import ConversationalResponseService
from src.application.services.conversational_llm_service import ConversationalConfig, ConversationalLLMService
from src.application.config.simple_config import ApplicationConfig

def test_direct_conversational():
    """Testa o conversational service diretamente"""
    
    print("🚀 Testando ConversationalResponseService diretamente...")
    
    # Criar service com configuração centralizada
    app_config = ApplicationConfig()
    conv_config = ConversationalConfig.from_application_config(app_config)
    conv_llm_service = ConversationalLLMService(conv_config)
    service = ConversationalResponseService(conv_llm_service)
    
    # Dados simulados
    user_query = "Quantas pessoas morreram?"
    sql_query = "SELECT COUNT(*) as total_mortes FROM sus_data WHERE MORTE = 1;"
    sql_results = [{'total_mortes': 2202}]
    
    print("🔄 Chamando generate_response...")
    
    try:
        response = service.generate_response(
            user_query=user_query,
            sql_query=sql_query,
            sql_results=sql_results,
            session_id="test_session"
        )
        
        print(f"✅ Resposta: {response.message}")
        print(f"📊 Tipo: {response.response_type}")
        print(f"🎯 Confiança: {response.confidence_score}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_direct_conversational()