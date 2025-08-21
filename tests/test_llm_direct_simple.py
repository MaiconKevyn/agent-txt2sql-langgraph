#!/usr/bin/env python3
"""
TESTE SIMPLES: LLM direto sem workflow
"""

import sys
sys.path.append('..')

def test_llm_raw():
    print("🧪 TESTE: LLM Raw - Llama3.1:8b")
    print("=" * 50)
    
    try:
        from langchain_ollama import ChatOllama
        from langchain_core.messages import HumanMessage, SystemMessage
        
        # Criar LLM
        llm = ChatOllama(
            model="llama3.1:8b",
            temperature=0.1
        )
        
        print("✅ LLM criado")
        
        # Prompt simples para SQL
        system_prompt = """You are a SQL expert. Generate SQL queries for a database with this table:

Table: pacientes  
Columns: id (INTEGER), nome (TEXT), idade (INTEGER), sexo (TEXT), cidade (TEXT)

Generate ONLY the SQL query, no explanation."""

        user_query = "Quantos pacientes existem?"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query)
        ]
        
        print(f"🧪 Query: {user_query}")
        print("🔄 Invocando LLM...")
        
        response = llm.invoke(messages)
        
        print(f"📊 RESPOSTA:")
        print(f"Tipo: {type(response)}")
        print(f"Content: '{response.content}'")
        print(f"Comprimento: {len(response.content) if response.content else 0}")
        
        if hasattr(response, 'content') and response.content:
            content = response.content.strip()
            if 'SELECT' in content.upper():
                print("✅ SQL detectado!")
            else:
                print("❌ Não parece SQL")
        else:
            print("❌ Resposta vazia")
            
        return response.content if hasattr(response, 'content') else str(response)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_different_prompts():
    print("\n🧪 TESTE: Diferentes prompts")
    print("=" * 50)
    
    try:
        from langchain_ollama import ChatOllama
        from langchain_core.messages import HumanMessage
        
        llm = ChatOllama(model="llama3.1:8b", temperature=0.1)
        
        prompts = [
            "Generate SQL: How many patients exist in table pacientes?",
            "SELECT COUNT(*) FROM pacientes;",
            "Escreva uma query SQL para contar pacientes na tabela 'pacientes'",
            "SQL query to count rows in pacientes table:"
        ]
        
        for i, prompt in enumerate(prompts, 1):
            print(f"\n{i}. Prompt: {prompt}")
            
            try:
                response = llm.invoke([HumanMessage(content=prompt)])
                content = response.content.strip() if response.content else ""
                print(f"   Resposta: '{content}'")
                
                if content and len(content) > 0:
                    print(f"   ✅ Obteve resposta ({len(content)} chars)")
                else:
                    print(f"   ❌ Resposta vazia")
                    
            except Exception as e:
                print(f"   ❌ Erro: {e}")
                
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    # Teste básico
    sql_result = test_llm_raw()
    
    # Teste com diferentes prompts
    test_different_prompts()