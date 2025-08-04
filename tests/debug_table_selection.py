#!/usr/bin/env python3
"""Debug do serviço de seleção de tabelas"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.application.services.table_selection_service import SUSTableSelectionService
from src.application.services.llm_communication_service import LLMCommunicationFactory

def debug_single_query():
    """Debug de uma única query"""
    print("🔍 Debug da seleção de tabelas...")
    
    # Configurar LLM service
    llm_service = LLMCommunicationFactory.create_ollama_service(
        model_name="llama3",
        temperature=0.0,
        timeout=60
    )
    
    # Criar serviço de seleção de tabelas
    table_selection_service = SUSTableSelectionService(llm_service)
    
    # Teste simples
    test_query = "O que significa o código CID I200?"
    
    print(f"🎯 Testando query: {test_query}")
    
    try:
        result = table_selection_service.select_tables_for_query(test_query)
        
        print(f"✅ Tabelas selecionadas: {result.selected_tables}")
        print(f"🎯 Confiança: {result.confidence}")
        print(f"📝 Justificativa: {result.justification}")
        print(f"🔄 Fallback usado: {result.fallback_used}")
        
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_single_query()