#!/usr/bin/env python3
"""
Teste do Sistema de Seleção Inteligente de Tabelas

Este script testa o novo fluxo de seleção inteligente de tabelas:
1. Pergunta → 2. Classificação → 3. LLM decide tabela → 4. Schema específico → 5. SQL
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.application.services.table_selection_service import SUSTableSelectionService
from src.application.services.llm_communication_service import LLMCommunicationFactory
from src.application.config.simple_config import ApplicationConfig


def test_table_selection():
    """Testa a seleção inteligente de tabelas com diferentes tipos de perguntas"""
    
    print("🧪 Iniciando teste de seleção inteligente de tabelas...")
    
    # Configurar LLM service
    llm_service = LLMCommunicationFactory.create_ollama_service(
        model_name="llama3",
        temperature=0.0,
        timeout=60
    )
    
    # Criar serviço de seleção de tabelas
    table_selection_service = SUSTableSelectionService(llm_service)
    
    # Casos de teste
    test_cases = [
        {
            "query": "Quantos pacientes existem no total?",
            "expected_tables": ["sus_data"],
            "description": "Pergunta simples sobre contagem - deve usar apenas sus_data"
        },
        {
            "query": "O que significa o código CID I200?",
            "expected_tables": ["cid_detalhado"],
            "description": "Pergunta sobre código específico - deve usar cid_detalhado"
        },
        {
            "query": "Quantos casos de doenças respiratórias tivemos?",
            "expected_tables": ["sus_data", "cid_capitulos"],
            "description": "Pergunta sobre categoria de doença - deve usar sus_data + cid_capitulos"
        },
        {
            "query": "Qual a idade média dos pacientes que morreram em Porto Alegre?",
            "expected_tables": ["sus_data"],
            "description": "Pergunta sobre dados demográficos - deve usar apenas sus_data"
        },
        {
            "query": "Quantas mortes por neoplasias tivemos por estado?",
            "expected_tables": ["sus_data", "cid_capitulos"],
            "description": "Pergunta sobre categoria + geografia - deve usar sus_data + cid_capitulos"
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📋 Teste {i}: {test_case['description']}")
        print(f"❓ Pergunta: {test_case['query']}")
        
        try:
            # Executar seleção
            selection_result = table_selection_service.select_tables_for_query(test_case['query'])
            
            print(f"✅ Tabelas selecionadas: {selection_result.selected_tables}")
            print(f"🎯 Confiança: {selection_result.confidence:.2f}")
            print(f"📝 Justificativa: {selection_result.justification}")
            print(f"🔄 Fallback usado: {selection_result.fallback_used}")
            
            # Verificar se a seleção é adequada
            is_correct = any(table in selection_result.selected_tables for table in test_case['expected_tables'])
            
            result = {
                "test_case": test_case,
                "selected_tables": selection_result.selected_tables,
                "confidence": selection_result.confidence,
                "justification": selection_result.justification,
                "fallback_used": selection_result.fallback_used,
                "is_adequate": is_correct
            }
            
            results.append(result)
            
            if is_correct:
                print("✅ Seleção adequada!")
            else:
                print(f"⚠️ Seleção pode não ser ideal. Esperado: {test_case['expected_tables']}")
                
        except Exception as e:
            print(f"❌ Erro no teste: {str(e)}")
            results.append({
                "test_case": test_case,
                "error": str(e),
                "is_adequate": False
            })
    
    # Resumo dos resultados
    print("\n" + "="*60)
    print("📊 RESUMO DOS RESULTADOS")
    print("="*60)
    
    successful_tests = sum(1 for r in results if r.get('is_adequate', False))
    total_tests = len(results)
    
    print(f"✅ Testes bem-sucedidos: {successful_tests}/{total_tests}")
    print(f"📈 Taxa de sucesso: {successful_tests/total_tests*100:.1f}%")
    
    for i, result in enumerate(results, 1):
        status = "✅" if result.get('is_adequate', False) else "❌"
        query = result['test_case']['query'][:50] + "..." if len(result['test_case']['query']) > 50 else result['test_case']['query']
        
        if 'error' in result:
            print(f"{status} Teste {i}: {query} - ERRO: {result['error']}")
        else:
            confidence = result.get('confidence', 0)
            tables = result.get('selected_tables', [])
            print(f"{status} Teste {i}: {query}")
            print(f"    Tabelas: {tables}, Confiança: {confidence:.2f}")
    
    return results


if __name__ == "__main__":
    try:
        results = test_table_selection()
        print(f"\n🎉 Teste concluído! Resultados salvos.")
        
    except Exception as e:
        print(f"❌ Erro durante execução do teste: {str(e)}")
        import traceback
        traceback.print_exc()