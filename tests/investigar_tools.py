#!/usr/bin/env python3
"""
INVESTIGAÇÃO: Quais tools a LLM está chamando e onde estão
"""

import sys
sys.path.append('..')

def investigar_tools_disponiveis():
    print("🔧 INVESTIGAÇÃO: Tools disponíveis para a LLM")
    print("=" * 60)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        
        # Criar LLM manager
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        print("✅ LLM Manager criado")
        
        # Obter tools SQL
        sql_tools = llm_manager.get_sql_tools()
        
        print(f"\n🔧 TOOLS SQL DISPONÍVEIS: {len(sql_tools)}")
        print("=" * 50)
        
        for i, tool in enumerate(sql_tools, 1):
            print(f"\n{i}. TOOL: {tool.name}")
            print(f"   📝 Descrição: {tool.description}")
            print(f"   📊 Tipo: {type(tool)}")
            print(f"   🏷️  Classe: {tool.__class__.__module__}.{tool.__class__.__name__}")
            
            # Verificar argumentos se disponível
            if hasattr(tool, 'args_schema') and tool.args_schema:
                print(f"   📋 Schema de argumentos: {tool.args_schema}")
            
            # Verificar se tem função
            if hasattr(tool, 'func'):
                print(f"   ⚙️  Função: {tool.func}")
        
        return sql_tools
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return []

def investigar_toolkit_origem():
    print("\n🗃️ INVESTIGAÇÃO: Origem do SQLDatabaseToolkit")
    print("=" * 60)
    
    try:
        from langchain_community.agent_toolkits import SQLDatabaseToolkit
        from langchain_community.utilities import SQLDatabase
        from langchain_ollama import ChatOllama
        
        # Criar componentes
        db = SQLDatabase.from_uri("sqlite:///sus_database.db")
        llm = ChatOllama(model="llama3.1:8b")
        
        # Criar toolkit
        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        
        print(f"📦 SQLDatabaseToolkit: {toolkit}")
        print(f"🏷️  Módulo: {toolkit.__class__.__module__}")
        print(f"📍 Arquivo: {toolkit.__class__.__module__.replace('.', '/')}.py")
        
        # Investigar métodos
        print(f"\n🔍 MÉTODOS DO TOOLKIT:")
        for method_name in dir(toolkit):
            if not method_name.startswith('_'):
                method = getattr(toolkit, method_name)
                if callable(method):
                    print(f"   ⚙️  {method_name}: {method}")
        
        # Investigar tools específicas
        tools = toolkit.get_tools()
        print(f"\n🔧 TOOLS GERADAS PELO TOOLKIT:")
        
        for tool in tools:
            print(f"\n   🔧 {tool.name}:")
            print(f"      📝 {tool.description}")
            print(f"      🏷️  {tool.__class__.__module__}.{tool.__class__.__name__}")
            
            # Tentar identificar função subjacente
            if hasattr(tool, 'func'):
                func = tool.func
                if hasattr(func, '__name__'):
                    print(f"      ⚙️  Função: {func.__name__}")
                if hasattr(func, '__module__'):
                    print(f"      📍 Módulo da função: {func.__module__}")
                    
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

def rastrear_tool_calls_reais():
    print("\n📞 INVESTIGAÇÃO: Tool calls em ação")
    print("=" * 60)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from langchain_core.messages import HumanMessage, SystemMessage
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Prompt que força tool usage
        system_prompt = """You are a SQL assistant. You have access to database tools. 
Use the tools to answer questions about the database."""
        
        user_query = "What tables are available in the database?"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_query)
        ]
        
        print(f"🧪 Query: {user_query}")
        print("🔄 Invocando LLM com tools...")
        
        result = llm_manager.invoke_with_tools(messages)
        
        print(f"\n📊 RESULTADO:")
        print(f"✅ Sucesso: {not result.get('error')}")
        print(f"🔧 Tem tool calls: {result.get('has_tool_calls')}")
        print(f"📞 Número de tool calls: {len(result.get('tool_calls', []))}")
        
        # Analisar cada tool call
        tool_calls = result.get('tool_calls', [])
        for i, tool_call in enumerate(tool_calls, 1):
            print(f"\n📞 TOOL CALL {i}:")
            print(f"   🏷️  Nome: {tool_call.get('name')}")
            print(f"   📋 Argumentos: {tool_call.get('args')}")
            print(f"   🆔 ID: {tool_call.get('id')}")
            print(f"   📊 Tipo: {tool_call.get('type')}")
        
        # Verificar resposta
        response = result.get('response')
        if response:
            print(f"\n💬 RESPOSTA LLM:")
            print(f"   📝 Content: '{response.content}'")
            if hasattr(response, 'tool_calls'):
                print(f"   🔧 Tool calls na resposta: {len(response.tool_calls)}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

def verificar_binding_tools():
    print("\n🔗 INVESTIGAÇÃO: Como tools são bound ao LLM")
    print("=" * 60)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Verificar LLM normal vs bound
        normal_llm = llm_manager._llm
        bound_llm = llm_manager.get_bound_llm()
        
        print(f"🤖 LLM Normal: {type(normal_llm)}")
        print(f"🔗 LLM Bound: {type(bound_llm)}")
        print(f"📊 São o mesmo objeto: {normal_llm is bound_llm}")
        
        # Verificar métodos disponíveis
        print(f"\n🔍 MÉTODOS DO LLM BOUND:")
        bound_methods = [method for method in dir(bound_llm) if not method.startswith('_')]
        for method in bound_methods[:10]:  # Primeiros 10
            print(f"   ⚙️  {method}")
        
        # Verificar se tem informações sobre tools
        if hasattr(bound_llm, 'bound_tools'):
            print(f"🔧 Bound tools: {bound_llm.bound_tools}")
        
        if hasattr(bound_llm, 'kwargs'):
            print(f"📋 Kwargs: {bound_llm.kwargs}")
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Investigação 1: Tools disponíveis
    tools = investigar_tools_disponiveis()
    
    # Investigação 2: Origem do toolkit
    investigar_toolkit_origem()
    
    # Investigação 3: Tool calls reais
    rastrear_tool_calls_reais()
    
    # Investigação 4: Binding process
    verificar_binding_tools()
    
    print(f"\n🎯 RESUMO: {len(tools)} tools SQL identificadas")
    print("📍 Localização: langchain_community.agent_toolkits.SQLDatabaseToolkit")
    print("🔧 Binding: llm.bind_tools() no HybridLLMManager")