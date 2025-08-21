#!/usr/bin/env python3
"""
Test Manual Tool Flow
Teste manual do fluxo: LLM → Enhanced Tables Tool → Decisão de tabela

Este script permite testar isoladamente:
1. Envio de pergunta para LLM
2. Chamada manual da Enhanced Tables Tool
3. Decisão da LLM sobre qual tabela usar

Uso: python tests/test_manual_tool_flow.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_community.utilities import SQLDatabase
from src.langgraph_migration.tools.enhanced_list_tables_tool import EnhancedListTablesTool
from src.langgraph_migration.llm_manager import HybridLLMManager
from src.application.config.simple_config import ApplicationConfig


def test_enhanced_tables_tool():
    """Testa a Enhanced Tables Tool isoladamente"""
    print("🔧 TESTE 1: ENHANCED TABLES TOOL ISOLADA")
    print("=" * 60)
    
    try:
        # Configurar database
        config = ApplicationConfig()
        db = SQLDatabase.from_uri(f"sqlite:///{config.database_path}")
        
        # Criar tool enhanced
        enhanced_tool = EnhancedListTablesTool(db=db)
        
        # Executar tool
        print("📋 Executando Enhanced Tables Tool...")
        result = enhanced_tool._run("")
        
        print(f"📤 RESULTADO ({len(result)} chars):")
        print("-" * 40)
        print(result)
        print("-" * 40)
        
        return result, enhanced_tool
        
    except Exception as e:
        print(f"❌ Erro no teste da tool: {e}")
        return None, None


def test_llm_table_selection(enhanced_output: str, user_question: str):
    """Testa seleção de tabelas pelo LLM usando output da enhanced tool"""
    print(f"\n🤖 TESTE 2: LLM DECISÃO DE TABELA")
    print("=" * 60)
    
    try:
        # Inicializar LLM Manager
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # Criar prompt para seleção de tabela
        selection_prompt = f"""Você é um especialista em bancos de dados SUS (Sistema Único de Saúde brasileiro).

TABELAS DISPONÍVEIS:
{enhanced_output}

PERGUNTA DO USUÁRIO: "{user_question}"

TAREFA: Analise a pergunta e selecione a(s) tabela(s) mais adequada(s).

RESPONDA APENAS com:
1. Nome da tabela escolhida
2. Razão da escolha (1 linha)

Exemplo:
TABELA: sus_data
RAZÃO: Pergunta sobre pacientes/mortes requer dados de atendimento SUS

SUA RESPOSTA:"""

        print("📝 PROMPT PARA LLM:")
        print("-" * 40)
        print(selection_prompt)
        print("-" * 40)
        
        # Enviar para LLM
        print("\n🚀 Enviando para LLM...")
        llm_response = llm_manager.generate_conversational_response(selection_prompt)
        
        print(f"🤖 RESPOSTA DA LLM:")
        print("-" * 40)
        print(llm_response)
        print("-" * 40)
        
        return llm_response
        
    except Exception as e:
        print(f"❌ Erro no teste LLM: {e}")
        return None


def test_complete_flow():
    """Testa o fluxo completo: Tool → LLM → Decisão"""
    print("🎯 TESTE COMPLETO: TOOL + LLM + DECISÃO")
    print("=" * 80)
    
    # Perguntas de teste
    test_questions = [
        "Quantos homens morreram?",
        "O que significa o código CID J44.0?",
        "Quantos pacientes existem por cidade?",
        "Qual é a descrição do CID para pneumonia?",
        "Quantas mulheres foram atendidas?"
    ]
    
    # Executar Enhanced Tool uma vez
    enhanced_output, tool = test_enhanced_tables_tool()
    
    if not enhanced_output:
        print("❌ Falha na tool enhanced, abortando testes")
        return
    
    # Testar cada pergunta
    for i, question in enumerate(test_questions, 1):
        print(f"\n🔍 TESTE {i}/5: {question}")
        print("-" * 50)
        
        llm_decision = test_llm_table_selection(enhanced_output, question)
        
        if llm_decision:
            print(f"✅ Teste {i} completo")
        else:
            print(f"❌ Teste {i} falhou")
        
        print()


def test_with_samples_enabled():
    """Testa com samples habilitados para comparar diferença"""
    print("\n🔬 TESTE EXTRA: COM SAMPLES HABILITADOS")
    print("=" * 60)
    
    # Modificar configuração temporariamente
    from src.application.config.table_descriptions import TOOL_CONFIGURATION
    
    # Backup original
    original_samples = TOOL_CONFIGURATION.get("include_samples", False)
    original_concise = TOOL_CONFIGURATION.get("concise_mode", True)
    
    try:
        # Habilitar samples temporariamente
        TOOL_CONFIGURATION["include_samples"] = True
        TOOL_CONFIGURATION["concise_mode"] = False  # Modo detalhado
        
        print("⚙️  Configuração temporária: include_samples=True, concise_mode=False")
        
        # Executar tool com samples
        enhanced_output_with_samples, _ = test_enhanced_tables_tool()
        
        if enhanced_output_with_samples:
            print(f"\n📊 COMPARAÇÃO DE TAMANHOS:")
            print(f"• Com samples: {len(enhanced_output_with_samples)} chars")
            print(f"• Diferença significativa em tokens!")
            
    finally:
        # Restaurar configuração original
        TOOL_CONFIGURATION["include_samples"] = original_samples
        TOOL_CONFIGURATION["concise_mode"] = original_concise
        print("\n✅ Configuração original restaurada")


def main():
    """Função principal do teste"""
    print("🧪 TESTE MANUAL: ENHANCED TOOL + LLM FLOW")
    print("=" * 80)
    print("Este script testa o fluxo:")
    print("1. 🔧 Enhanced Tables Tool (output com descrições)")
    print("2. 🤖 LLM recebe output e pergunta do usuário")
    print("3. 🎯 LLM decide qual tabela usar")
    print("=" * 80)
    
    try:
        # Executar testes
        test_complete_flow()
        
        # Teste extra com samples
        test_with_samples_enabled()
        
        print("\n🎉 TODOS OS TESTES CONCLUÍDOS!")
        print("💡 Use este script para testar diferentes perguntas e configurações")
        
    except Exception as e:
        print(f"❌ Erro geral nos testes: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()