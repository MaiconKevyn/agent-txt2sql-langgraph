#!/usr/bin/env python3
"""
DEBUG WORKFLOW: Rastrear cada passo do workflow
"""

import sys
sys.path.append('..')

def debug_workflow_detalhado():
    print("🔍 DEBUG: Rastreamento detalhado do workflow")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.workflow_v3 import create_production_sql_agent
        from src.langgraph_migration.state_v3 import create_initial_messages_state
        
        # Criar agent
        print("🤖 Criando agent...")
        agent = create_production_sql_agent()
        
        # Criar estado inicial
        print("📋 Criando estado inicial...")
        query = "Quantos pacientes existem na tabela pacientes?"
        state = create_initial_messages_state(query)
        
        print(f"📝 Query: {query}")
        print(f"📊 Estado inicial criado")
        
        # Executar workflow com debug
        print("\n🔄 Executando workflow...")
        
        # Interceptar o workflow para ver cada passo
        final_state = agent.invoke(state)
        
        print(f"\n📊 ESTADO FINAL:")
        print(f"✅ Success: {final_state.get('success')}")
        print(f"🗃️ Generated SQL: {final_state.get('generated_sql')}")
        print(f"✅ Validated SQL: {final_state.get('validated_sql')}")
        print(f"📊 SQL Execution Result: {final_state.get('sql_execution_result')}")
        print(f"💬 Final Response: {final_state.get('final_response')}")
        print(f"📋 Current Phase: {final_state.get('current_phase')}")
        print(f"🔄 Query Route: {final_state.get('query_route')}")
        
        # Verificar fases completadas
        phases = final_state.get('phases_completed', [])
        print(f"\n📋 Fases completadas: {len(phases)}")
        for phase in phases:
            print(f"  ✅ {phase}")
        
        # Verificar mensagens
        messages = final_state.get('messages', [])
        print(f"\n💬 Mensagens: {len(messages)}")
        for i, msg in enumerate(messages):
            msg_type = type(msg).__name__
            content = getattr(msg, 'content', str(msg))[:100]
            print(f"  {i+1}. {msg_type}: {content}...")
        
        # Verificar tool calls
        tool_calls = final_state.get('tool_call_results', [])
        print(f"\n🔧 Tool calls: {len(tool_calls)}")
        for i, tool_call in enumerate(tool_calls):
            if hasattr(tool_call, 'tool_name'):
                print(f"  {i+1}. {tool_call.tool_name}: {tool_call.success}")
            else:
                print(f"  {i+1}. {tool_call}")
        
        # Verificar erros
        errors = final_state.get('errors', [])
        if errors:
            print(f"\n❌ Erros: {len(errors)}")
            for error in errors:
                print(f"  🔴 {error}")
        
        return final_state
        
    except Exception as e:
        print(f"❌ Erro no debug: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_llm_direct():
    print("\n🧪 TESTE DIRETO: LLM Manager SQL Generation")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from src.application.config.simple_config import ApplicationConfig
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Simular schema context
        schema_context = """
        Table: pacientes
        Columns: id (INTEGER), nome (TEXT), idade (INTEGER), sexo (TEXT), cidade (TEXT)
        Sample rows: 
        (1, 'João Silva', 45, 'M', 'São Paulo')
        (2, 'Maria Santos', 32, 'F', 'Rio de Janeiro')
        """
        
        print("🗃️ Schema context preparado")
        print("🧪 Testando geração SQL direta...")
        
        result = llm_manager.generate_sql_query(
            user_query="Quantos pacientes existem?",
            schema_context=schema_context
        )
        
        print(f"📊 Resultado direto:")
        print(f"✅ Success: {result.get('success')}")
        print(f"🗃️ SQL: '{result.get('sql_query')}'")
        print(f"❌ Error: {result.get('error')}")
        
        # Verificar mensagens
        messages = result.get('messages', [])
        print(f"💬 Mensagens: {len(messages)}")
        for i, msg in enumerate(messages):
            msg_type = type(msg).__name__
            content = getattr(msg, 'content', str(msg))
            print(f"  {i+1}. {msg_type}: {content}")
        
    except Exception as e:
        print(f"❌ Erro no teste direto: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Teste 1: Workflow completo
    final_state = debug_workflow_detalhado()
    
    # Teste 2: LLM direto
    test_llm_direct()