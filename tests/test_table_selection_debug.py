#!/usr/bin/env python3
"""
Debug da Seleção Inteligente de Tabelas
Testa especificamente a função de seleção para identificar o problema

Uso: python tests/test_table_selection_debug.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.langgraph_migration.llm_manager import HybridLLMManager
from src.application.config.simple_config import ApplicationConfig
from langchain_core.messages import HumanMessage


def test_table_selection_direct():
    """Testa seleção de tabelas diretamente"""
    print("🎯 DEBUG: TABLE SELECTION DIRECT TEST")
    print("=" * 60)
    
    try:
        # Inicializar
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # Obter Enhanced Tool output
        tools = llm_manager.get_sql_tools()
        list_tool = next((tool for tool in tools if tool.name == "sql_db_list_tables"), None)
        tool_result = list_tool.invoke("")
        
        print(f"📋 Enhanced Tool Output ({len(tool_result)} chars):")
        print(f"{tool_result}")
        print()
        
        # Teste de queries diferentes
        test_queries = [
            "Quantos homens morreram?",
            "O que significa CID J44.0?", 
            "Qual cidade com mais mortes de homens?",
            "Mortes por descrição de doença"
        ]
        
        for query in test_queries:
            print(f"🔍 Testing: '{query}'")
            
            # Criar prompt
            selection_prompt = f"""You are a database expert. Analyze the user query and select ONLY the relevant tables needed.

AVAILABLE TABLES WITH DESCRIPTIONS:
{tool_result}

USER QUERY: "{query}"

TASK: Select ONLY the tables that are directly relevant to answer this specific query.

RULES:
1. If query is about SUS data (patients, deaths, cities, ages, diagnoses): select "sus_data"
2. If query is about CID meanings/descriptions: select "cid_detalhado" 
3. If query needs both SUS data AND CID descriptions: select both
4. NEVER select unnecessary tables

EXAMPLES:
- "Quantos homens morreram?" → sus_data (only patient data needed)
- "O que significa CID J44.0?" → cid_detalhado (only CID meaning needed)
- "Mortes por descrição de doença" → sus_data,cid_detalhado (need both for JOIN)

RESPONSE FORMAT: Just the table names separated by commas (e.g., "sus_data" or "sus_data,cid_detalhado")

SELECTED TABLES:"""

            # Testar LLM
            llm = llm_manager._llm
            response = llm.invoke([HumanMessage(content=selection_prompt)])
            
            selected_str = response.content.strip() if hasattr(response, 'content') else str(response)
            
            print(f"   LLM Response: '{selected_str}'")
            
            # Parse response
            selected_tables = []
            if selected_str:
                parsed_tables = [table.strip() for table in selected_str.split(',')]
                available_tables = ['cid_detalhado', 'sus_data']
                
                for table in parsed_tables:
                    if table in available_tables:
                        selected_tables.append(table)
            
            print(f"   Parsed Tables: {selected_tables}")
            print(f"   Expected for '{query}': {get_expected_tables(query)}")
            print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def get_expected_tables(query: str) -> list:
    """Retorna tabelas esperadas para uma query"""
    query_lower = query.lower()
    
    if "cid" in query_lower and ("significa" in query_lower or "descricao" in query_lower):
        return ["cid_detalhado"]
    elif "mortes por descrição" in query_lower or "doença" in query_lower:
        return ["sus_data", "cid_detalhado"]
    elif any(word in query_lower for word in ["homens", "mulheres", "morreram", "cidade", "pacientes"]):
        return ["sus_data"]
    else:
        return ["sus_data"]  # Default


def test_simple_selection():
    """Teste mais simples"""
    print("🔧 DEBUG: SIMPLE SELECTION TEST")
    print("=" * 60)
    
    try:
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        llm = llm_manager._llm
        
        # Prompt super simples
        simple_prompt = """You have these tables:
- sus_data: contains patient data, deaths, cities
- cid_detalhado: contains disease codes and descriptions

For the query "Quantos homens morreram?" which table(s) do you need?
Answer with just the table name(s): """

        response = llm.invoke([HumanMessage(content=simple_prompt)])
        print(f"Simple response: '{response.content}'")
        
    except Exception as e:
        print(f"❌ Simple test error: {e}")


def main():
    """Main function"""
    print("🐛 DEBUG: TABLE SELECTION SYSTEM")
    print("=" * 70)
    
    try:
        test_table_selection_direct()
        test_simple_selection()
        
    except Exception as e:
        print(f"❌ Main error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()