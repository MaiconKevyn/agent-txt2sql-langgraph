#!/usr/bin/env python3
"""
Debug da Geração SQL com ChatPromptTemplate
Testa especificamente a geração SQL para identificar o problema

Uso: python tests/test_sql_generation_debug.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_core.prompts import ChatPromptTemplate
from src.langgraph_migration.llm_manager import HybridLLMManager
from src.application.config.simple_config import ApplicationConfig
from src.application.config.table_templates import build_multi_table_prompt


def test_direct_sql_generation():
    """Testa geração SQL diretamente"""
    print("🤖 DEBUG: DIRECT SQL GENERATION")
    print("=" * 60)
    
    try:
        # Inicializar LLM (USAR LLM NÃO-BOUND para SQL generation)
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        llm = llm_manager._llm  # Use unbound LLM for direct SQL generation
        
        # Dados do teste
        user_query = "Quantos homens morreram?"
        selected_tables = ['cid_detalhado', 'sus_data']
        schema_context = """
CREATE TABLE cid_detalhado (
    id INTEGER, 
    codigo TEXT NOT NULL, 
    descricao TEXT NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (codigo)
)

CREATE TABLE sus_data (
    DIAG_PRINC TEXT,
    MUNIC_RES INTEGER,
    IDADE INTEGER,
    SEXO INTEGER,
    MORTE INTEGER,
    CIDADE_RESIDENCIA_PACIENTE TEXT
)
"""
        
        # Build table rules
        table_rules = build_multi_table_prompt(selected_tables)
        print(f"📏 Table rules length: {len(table_rules)} chars")
        print(f"📋 First 200 chars: {table_rules[:200]}...")
        
        # Criar ChatPromptTemplate
        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert assistant for Brazilian healthcare (SUS) data analysis.

📋 CORE INSTRUCTIONS:
1. Generate syntactically correct SQLite queries
2. Use proper table and column names from the schema
3. Handle Portuguese language questions appropriately
4. Return only the SQL query, no explanation
5. Use appropriate WHERE clauses for filtering
6. Include LIMIT clauses when appropriate (default LIMIT 100)
7. Use proper JOINs when querying multiple tables

🔍 DATABASE SCHEMA:
{schema_context}"""),
            
            ("system", "{table_specific_rules}"),
            
            ("human", "🎯 USER QUERY: {user_query}\n\nGenerate the SQL query:")
        ])
        
        # Format messages
        formatted_messages = sql_prompt_template.format_messages(
            schema_context=schema_context,
            table_specific_rules=table_rules,
            user_query=user_query
        )
        
        print(f"\n📨 FORMATTED MESSAGES ({len(formatted_messages)}):")
        for i, msg in enumerate(formatted_messages):
            print(f"  {i+1}. {msg.__class__.__name__}: {len(msg.content)} chars")
            print(f"     Preview: {msg.content[:100]}...")
        
        # Test LLM response
        print(f"\n🚀 INVOKING LLM...")
        response = llm.invoke(formatted_messages)
        
        print(f"📤 LLM RESPONSE:")
        print(f"  Type: {type(response)}")
        print(f"  Has content: {hasattr(response, 'content')}")
        
        if hasattr(response, 'content'):
            print(f"  Content length: {len(response.content) if response.content else 0}")
            print(f"  Content: '{response.content}'")
        else:
            print(f"  String representation: '{str(response)}'")
        
        # Clean SQL
        sql_query = response.content.strip() if hasattr(response, 'content') else str(response)
        cleaned_sql = llm_manager._clean_sql_query(sql_query)
        
        print(f"\n🧹 CLEANED SQL: '{cleaned_sql}'")
        
        return cleaned_sql
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_simple_prompt():
    """Testa um prompt mais simples"""
    print("\n🔧 DEBUG: SIMPLE PROMPT TEST")
    print("=" * 60)
    
    try:
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        llm = llm_manager._llm  # Use unbound LLM
        
        # Prompt simples
        simple_prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a SQL expert. Generate only SQL queries, no explanations."),
            ("human", "Generate SQL to count male deaths in sus_data table where SEXO=1 and MORTE=1")
        ])
        
        messages = simple_prompt.format_messages()
        print(f"📨 Simple prompt: {len(messages)} messages")
        
        response = llm.invoke(messages)
        sql = response.content.strip() if hasattr(response, 'content') else str(response)
        
        print(f"📤 Simple SQL: '{sql}'")
        
        return sql
        
    except Exception as e:
        print(f"❌ Simple test error: {e}")
        return None


def test_minimal_prompt():
    """Testa prompt minimal"""
    print("\n⚡ DEBUG: MINIMAL PROMPT TEST")
    print("=" * 60)
    
    try:
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        llm = llm_manager._llm  # Use unbound LLM
        
        # Prompt mínimo
        from langchain_core.messages import HumanMessage
        
        message = HumanMessage(content="SELECT COUNT(*) FROM sus_data WHERE SEXO = 1 AND MORTE = 1;")
        response = llm.invoke([message])
        
        print(f"📤 Minimal response: '{response.content if hasattr(response, 'content') else str(response)}'")
        
        return response
        
    except Exception as e:
        print(f"❌ Minimal test error: {e}")
        return None


def main():
    """Main function"""
    print("🐛 DEBUG: SQL GENERATION WITH CHATPROMPTTEMPLATE")
    print("=" * 70)
    
    try:
        # Test 1: Full prompt with templates
        sql1 = test_direct_sql_generation()
        
        # Test 2: Simple prompt
        sql2 = test_simple_prompt()
        
        # Test 3: Minimal prompt
        response3 = test_minimal_prompt()
        
        print(f"\n📊 RESULTS SUMMARY:")
        print(f"  Full template SQL: {'✅' if sql1 else '❌'} {sql1}")
        print(f"  Simple prompt SQL: {'✅' if sql2 else '❌'} {sql2}")
        print(f"  Minimal response: {'✅' if response3 else '❌'}")
        
        if not sql1 and not sql2:
            print(f"\n🚨 PROBLEM IDENTIFIED: LLM not responding to SQL prompts")
        elif sql1:
            print(f"\n✅ SUCCESS: ChatPromptTemplate working!")
        
    except Exception as e:
        print(f"❌ Main error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()