#!/usr/bin/env python3
"""
Mapeamento de Tools por Etapa do Fluxo
Mostra exatamente quando cada tool é chamada no pipeline

Uso: python tests/test_tool_flow_stages.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.langgraph_migration.llm_manager import HybridLLMManager
from src.application.config.simple_config import ApplicationConfig


def analyze_tool_flow():
    """Analisa o fluxo de tools no sistema"""
    print("🔄 MAPEAMENTO: TOOLS POR ETAPA DO FLUXO")
    print("=" * 80)
    
    try:
        # Inicializar para ver as tools
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        tools = llm_manager.get_sql_tools()
        
        print("📋 TOOLS DISPONÍVEIS:")
        for tool in tools:
            enhanced = "🚀" if "Enhanced" in type(tool).__name__ else "🔧"
            print(f"  {enhanced} {tool.name}")
        print()
        
        # Mapear cada etapa
        flow_stages = {
            "1. 🌐 API Request": {
                "description": "Recebe pergunta do usuário",
                "tools_called": [],
                "input": '{"question": "Quantos homens morreram?"}',
                "output": "user_query extraída"
            },
            
            "2. 🎭 Orchestrator Initialization": {
                "description": "Inicializa LangGraph workflow",
                "tools_called": [],
                "input": "user_query",
                "output": "MessagesState criado"
            },
            
            "3. 🔍 Query Classification": {
                "description": "Classifica tipo de query (DATABASE/CONVERSATIONAL)",
                "tools_called": [],
                "input": "user_query",
                "output": "query_route = DATABASE"
            },
            
            "4. 🗂️ Table Discovery": {
                "description": "Lista tabelas com descrições enhanced",
                "tools_called": ["🚀 sql_db_list_tables (Enhanced)"],
                "input": '""',  # Empty string
                "output": "Lista de tabelas + descrições (351 chars)",
                "details": [
                    "🚀 ÚNICA tool chamada nesta etapa",
                    "📤 Output: Tabelas + descrições + mapeamentos SUS",
                    "⚡ Performance: 351 chars (modo conciso)",
                    "🎯 Guia de seleção incluído"
                ]
            },
            
            "5. 📋 Schema Analysis": {
                "description": "Obtem schema das tabelas selecionadas",
                "tools_called": ["🔧 sql_db_schema"],
                "input": "sus_data",  # Tabelas selecionadas
                "output": "Schema detalhado + samples + enhanced SUS context",
                "details": [
                    "🔧 Tool padrão do LangChain",
                    "📤 Output: CREATE TABLE + sample rows",
                    "🚀 Enhanced: +2465 chars mapeamentos SUS",
                    "⚠️  Mapeamentos SEXO=1→Masculino adicionados"
                ]
            },
            
            "6. 🤖 SQL Generation": {
                "description": "LLM gera SQL usando contexto das tools",
                "tools_called": [],
                "input": "user_query + schema_context + enhanced_mappings",
                "output": "SELECT COUNT(*) FROM sus_data WHERE SEXO=1 AND MORTE=1;",
                "details": [
                    "🤖 LLM processa contexto das tools anteriores",
                    "📊 Usa enhanced context do sql_db_schema",
                    "🎯 Aplica mapeamentos: homens→SEXO=1, morreram→MORTE=1"
                ]
            },
            
            "7. ✅ SQL Validation": {
                "description": "Valida SQL antes de executar",
                "tools_called": ["🔧 sql_db_query_checker"],
                "input": "SELECT COUNT(*) FROM sus_data WHERE SEXO=1 AND MORTE=1;",
                "output": "Query is valid",
                "details": [
                    "🔧 Tool padrão do LangChain",
                    "✅ Verifica sintaxe SQL",
                    "🛡️  Previne queries maliciosas"
                ]
            },
            
            "8. 🏃 SQL Execution": {
                "description": "Executa SQL validada no banco",
                "tools_called": ["🔧 sql_db_query"],
                "input": "SELECT COUNT(*) FROM sus_data WHERE SEXO=1 AND MORTE=1;",
                "output": "[(9341,)]",
                "details": [
                    "🔧 Tool padrão do LangChain",
                    "🗄️  Executa no SQLite database",
                    "📊 Retorna resultado bruto"
                ]
            },
            
            "9. 🎨 Response Formatting": {
                "description": "Formata resposta em linguagem natural",
                "tools_called": [],
                "input": "user_query + sql_result: [(9341,)]",
                "output": "9.341 homens morreram no sistema de saúde.",
                "details": [
                    "🤖 LLM secundário formata resultado",
                    "📝 Converte dados técnicos em linguagem natural",
                    "🎯 Resposta concisa e compreensível"
                ]
            },
            
            "10. 📤 API Response": {
                "description": "Retorna JSON final",
                "tools_called": [],
                "input": "formatted_response + metadata",
                "output": "JSON com response, sql_query, results, etc.",
                "details": [
                    "🌐 Formata resposta HTTP",
                    "📊 Inclui metadata completa",
                    "⏱️  Tempo total: ~12.5s"
                ]
            }
        }
        
        # Exibir análise detalhada
        for stage, info in flow_stages.items():
            print(f"{stage}")
            print(f"📝 {info['description']}")
            print(f"📥 Input: {info['input']}")
            print(f"📤 Output: {info['output']}")
            
            if info['tools_called']:
                print(f"🔧 Tools chamadas: {', '.join(info['tools_called'])}")
            else:
                print("🔧 Tools chamadas: Nenhuma (processamento LLM)")
            
            if 'details' in info:
                for detail in info['details']:
                    print(f"   {detail}")
            
            print("-" * 60)
        
        # Resumo de uso das tools
        print("\n📊 RESUMO: USO DAS TOOLS")
        print("=" * 50)
        
        tool_usage = {
            "🚀 sql_db_list_tables (Enhanced)": {
                "etapa": "4. Table Discovery",
                "frequencia": "1x por query",
                "input": '""',
                "output_size": "351 chars",
                "critico": "SIM - Decide quais tabelas usar"
            },
            "🔧 sql_db_schema": {
                "etapa": "5. Schema Analysis", 
                "frequencia": "1x por tabela selecionada",
                "input": "nome_da_tabela",
                "output_size": "~2500 chars (com enhanced SUS)",
                "critico": "SIM - Fornece contexto para SQL"
            },
            "🔧 sql_db_query_checker": {
                "etapa": "7. SQL Validation",
                "frequencia": "1x por query gerada",
                "input": "sql_query",
                "output_size": "~50 chars",
                "critico": "MÉDIO - Segurança"
            },
            "🔧 sql_db_query": {
                "etapa": "8. SQL Execution",
                "frequencia": "1x por query validada",
                "input": "sql_query",
                "output_size": "Variável (resultado)",
                "critico": "SIM - Executa no banco"
            }
        }
        
        for tool_name, usage in tool_usage.items():
            print(f"\n{tool_name}")
            print(f"  📍 Etapa: {usage['etapa']}")
            print(f"  🔄 Frequência: {usage['frequencia']}")
            print(f"  📥 Input: {usage['input']}")
            print(f"  📊 Output: {usage['output_size']}")
            print(f"  ⚡ Crítico: {usage['critico']}")
        
        # Fluxo visual
        print(f"\n🎯 FLUXO VISUAL DAS TOOLS")
        print("=" * 50)
        print("Pergunta → Classification → 🚀Enhanced List → 🔧Schema → LLM → 🔧Checker → 🔧Query → Resposta")
        print("                             ↓                ↓                 ↓         ↓")
        print("                           Tabelas         Contexto         Valida    Executa")
        print("                           (351c)          (2500c)           SQL       SQL")
        
    except Exception as e:
        print(f"❌ Erro na análise: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Função principal"""
    print("🔄 ANÁLISE: TOOLS POR ETAPA DO SISTEMA")
    print("=" * 80)
    print("Este script mostra:")
    print("• 📍 Quando cada tool é chamada")
    print("• 📥 Input de cada tool")
    print("• 📤 Output de cada tool") 
    print("• 🎯 Importância de cada tool")
    print("=" * 80)
    
    analyze_tool_flow()


if __name__ == "__main__":
    main()