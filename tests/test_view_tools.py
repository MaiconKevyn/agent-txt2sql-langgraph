#!/usr/bin/env python3
"""
Visualizar Tools Disponíveis
Script para listar e testar tools individualmente

Uso: python tests/test_view_tools.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.langgraph_migration.llm_manager import HybridLLMManager
from src.application.config.simple_config import ApplicationConfig


def list_all_tools():
    """Lista todas as tools disponíveis"""
    print("🔧 LISTANDO TODAS AS TOOLS DISPONÍVEIS")
    print("=" * 60)
    
    try:
        # Inicializar LLM Manager
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # Obter tools SQL
        sql_tools = llm_manager.get_sql_tools()
        
        print(f"📊 Total de tools: {len(sql_tools)}")
        print("-" * 40)
        
        for i, tool in enumerate(sql_tools, 1):
            print(f"{i}. 🔧 {tool.name}")
            print(f"   📝 {tool.description[:100]}...")
            print(f"   🏷️  Tipo: {type(tool).__name__}")
            
            # Destacar nossa Enhanced Tool
            if "Enhanced" in type(tool).__name__:
                print(f"   🚀 TOOL CUSTOMIZADA!")
            print()
        
        return sql_tools
        
    except Exception as e:
        print(f"❌ Erro ao listar tools: {e}")
        return None


def test_specific_tool(tools, tool_name: str):
    """Testa uma tool específica"""
    print(f"\n🧪 TESTANDO TOOL: {tool_name}")
    print("=" * 50)
    
    # Encontrar tool
    target_tool = None
    for tool in tools:
        if tool.name == tool_name:
            target_tool = tool
            break
    
    if not target_tool:
        print(f"❌ Tool '{tool_name}' não encontrada")
        return
    
    try:
        print(f"🔧 Executando: {tool_name}")
        print("📝 Descrição completa:")
        print(f"   {target_tool.description}")
        print()
        
        # Executar tool
        if tool_name == "sql_db_list_tables":
            print("📋 Executando sql_db_list_tables...")
            result = target_tool._run("")
            print(f"📤 RESULTADO ({len(result)} chars):")
            print("-" * 30)
            print(result)
            print("-" * 30)
            
        elif tool_name == "sql_db_schema":
            print("📋 Executando sql_db_schema para 'sus_data'...")
            result = target_tool._run("sus_data")
            print(f"📤 RESULTADO ({len(result)} chars):")
            print("-" * 30)
            print(result[:500] + "..." if len(result) > 500 else result)
            print("-" * 30)
            
        elif tool_name == "sql_db_query":
            print("📋 Executando sql_db_query com query simples...")
            test_query = "SELECT COUNT(*) FROM sus_data"
            result = target_tool._run(test_query)
            print(f"📤 RESULTADO:")
            print("-" * 30)
            print(result)
            print("-" * 30)
            
        elif tool_name == "sql_db_query_checker":
            print("📋 Executando sql_db_query_checker...")
            test_query = "SELECT COUNT(*) FROM sus_data"
            result = target_tool._run(test_query)
            print(f"📤 RESULTADO:")
            print("-" * 30)
            print(result)
            print("-" * 30)
            
        else:
            print(f"⚠️  Tool {tool_name} não tem teste implementado")
            
    except Exception as e:
        print(f"❌ Erro ao testar tool: {e}")


def interactive_tool_tester():
    """Interface interativa para testar tools"""
    print("\n🎯 TESTE INTERATIVO DE TOOLS")
    print("=" * 50)
    
    # Listar tools
    tools = list_all_tools()
    if not tools:
        return
    
    # Menu de opções
    while True:
        print(f"\n📋 TOOLS DISPONÍVEIS:")
        for i, tool in enumerate(tools, 1):
            enhanced = "🚀" if "Enhanced" in type(tool).__name__ else "🔧"
            print(f"{i}. {enhanced} {tool.name}")
        
        print(f"{len(tools) + 1}. 🚪 Sair")
        
        try:
            choice = input(f"\nEscolha uma tool (1-{len(tools) + 1}): ").strip()
            
            if choice == str(len(tools) + 1):
                print("👋 Saindo...")
                break
                
            tool_index = int(choice) - 1
            if 0 <= tool_index < len(tools):
                selected_tool = tools[tool_index]
                test_specific_tool(tools, selected_tool.name)
            else:
                print("❌ Opção inválida")
                
        except (ValueError, KeyboardInterrupt):
            print("\n👋 Saindo...")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")


def main():
    """Função principal"""
    print("🔧 VISUALIZADOR DE TOOLS")
    print("=" * 50)
    print("Este script permite:")
    print("1. 📋 Listar todas as tools disponíveis")
    print("2. 🧪 Testar tools individuais")
    print("3. 🎯 Interface interativa")
    print("=" * 50)
    
    try:
        # Listar todas as tools
        tools = list_all_tools()
        
        if tools:
            # Testar automaticamente a Enhanced Tool
            print("\n🚀 TESTANDO ENHANCED TOOL AUTOMATICAMENTE:")
            test_specific_tool(tools, "sql_db_list_tables")
            
            # Perguntar se quer modo interativo
            response = input("\n❓ Deseja testar outras tools? (s/n): ").strip().lower()
            if response in ['s', 'sim', 'y', 'yes']:
                interactive_tool_tester()
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()