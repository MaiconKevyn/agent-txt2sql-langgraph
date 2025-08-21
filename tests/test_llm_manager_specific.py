#!/usr/bin/env python3
"""
TESTE ESPECÍFICO: LLM Manager generate_sql_query
"""

import sys
sys.path.append('..')

def test_llm_manager_sql_generation():
    print("🧪 TESTE: LLM Manager - generate_sql_query")
    print("=" * 60)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        
        # Criar config e LLM manager
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        print("✅ LLM Manager criado")
        
        # Schema simples
        schema_context = """
Table: pacientes
Columns: id (INTEGER), nome (TEXT), idade (INTEGER), sexo (TEXT), cidade (TEXT)
Sample rows:
(1, 'João Silva', 45, 'M', 'São Paulo')
(2, 'Maria Santos', 32, 'F', 'Rio de Janeiro')
"""
        
        # Teste query
        user_query = "Quantos pacientes existem?"
        
        print(f"🧪 Query: {user_query}")
        print("🔄 Chamando generate_sql_query...")
        
        # Chamar método
        result = llm_manager.generate_sql_query(
            user_query=user_query,
            schema_context=schema_context
        )
        
        print(f"\n📊 RESULTADO COMPLETO:")
        print(f"Success: {result.get('success')}")
        print(f"SQL Query: '{result.get('sql_query')}'")
        print(f"Error: {result.get('error')}")
        
        # Verificar mensagens internas
        messages = result.get('messages', [])
        print(f"\nMensagens ({len(messages)}):")
        for i, msg in enumerate(messages):
            msg_type = type(msg).__name__
            content = getattr(msg, 'content', str(msg))
            print(f"  {i+1}. {msg_type}: '{content}'")
        
        # Verificar tool calls
        tool_calls = result.get('tool_calls', [])
        print(f"\nTool calls: {len(tool_calls)}")
        for call in tool_calls:
            print(f"  - {call}")
        
        # Se houver sucesso mas SQL vazio, investigar
        if result.get('success') and not result.get('sql_query'):
            print(f"\n🔍 INVESTIGAÇÃO: Sucesso mas SQL vazio")
            
            # Verificar última mensagem AI
            ai_messages = [msg for msg in messages if hasattr(msg, 'content') and msg.__class__.__name__ == 'AIMessage']
            if ai_messages:
                last_ai = ai_messages[-1]
                print(f"Última AI message: '{last_ai.content}'")
                
                # Testar limpeza manual
                cleaned = llm_manager._clean_sql_query(last_ai.content)
                print(f"Limpeza manual: '{cleaned}'")
            
        return result
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_invoke_with_tools():
    print("\n🔧 TESTE: invoke_with_tools direto")
    print("=" * 50)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from langchain_core.messages import SystemMessage, HumanMessage
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Criar mensagens simples
        system_prompt = "You are a SQL expert. Generate ONLY SQL queries, no explanations."
        user_query = "Quantos pacientes existem na tabela pacientes?"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query)
        ]
        
        print("🔄 Chamando invoke_with_tools...")
        
        result = llm_manager.invoke_with_tools(messages)
        
        print(f"📊 RESULTADO:")
        print(f"Response: {result.get('response')}")
        print(f"Has tool calls: {result.get('has_tool_calls')}")
        print(f"Error: {result.get('error')}")
        
        if result.get('response'):
            response = result['response']
            content = getattr(response, 'content', str(response))
            print(f"Content: '{content}'")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Teste principal
    result = test_llm_manager_sql_generation()
    
    # Teste invoke_with_tools
    test_invoke_with_tools()