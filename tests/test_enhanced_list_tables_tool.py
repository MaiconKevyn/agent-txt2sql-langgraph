#!/usr/bin/env python3
"""
Teste da EnhancedListTablesTool - PostgreSQL SIH-RS
Testa a mesma tool que a LLM chama para ver tabelas e decidir qual usar
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.application.config.simple_config import ApplicationConfig
from src.langgraph_migration.llm_manager import HybridLLMManager
from src.langgraph_migration.tools.enhanced_list_tables_tool import EnhancedListTablesTool

def test_enhanced_list_tables_tool():
    """Testa a EnhancedListTablesTool com PostgreSQL"""
    
    print("=== TESTE ENHANCED LIST TABLES TOOL - POSTGRESQL ===\n")
    
    try:
        # 1. Configurar PostgreSQL
        config = ApplicationConfig()
        print(f"Database: {config.database_type}")
        print(f"URI: {config.database_path}")
        
        # 2. Inicializar LLM Manager
        print("\n1. Inicializando HybridLLMManager...")
        llm_manager = HybridLLMManager(config)
        sql_db = llm_manager.get_database()
        print("✅ LLM Manager inicializado com sucesso!")
        
        # 3. Criar EnhancedListTablesTool
        print("\n2. Criando EnhancedListTablesTool...")
        tool = EnhancedListTablesTool(sql_db)
        print("✅ Tool criada com sucesso!")
        print(f"Nome da tool: {tool.name}")
        print(f"Descrição: {tool.description}")
        
        # 4. Testar execução da tool (mesmo que LLM faz)
        print("\n--- TESTE: Execução da Tool (como LLM faz) ---")
        
        # Executar tool (mesmo input que LLM usa)
        tool_input = ""  # EnhancedListTablesTool não precisa de input
        
        print("🚀 Executando tool...")
        
        # Executar tool (assinatura correta)
        result = tool._run(tool_input)
        
        print("✅ Tool executada com sucesso!")
        print(f"📏 Tamanho do resultado: {len(result)} caracteres")
        print(f"📄 Linhas: {result.count(chr(10)) + 1}")
        
        # Mostrar resultado completo (limitado para legibilidade)
        print("\n📋 RESULTADO COMPLETO DA TOOL:")
        print("-" * 80)
        print(result)
        print("-" * 80)
        
        # Contar tabelas mencionadas
        print("\n📊 ANÁLISE DO RESULTADO:")
        table_keywords = ["internacoes", "cid10", "municipios", "mortes", "hospital"]
        found_tables = []
        for keyword in table_keywords:
            if keyword.lower() in result.lower():
                found_tables.append(keyword)
        
        print(f"🎯 Tabelas principais encontradas: {found_tables}")
        print(f"📋 Total de tabelas mencionadas: {len(found_tables)}")
        
        # Verificar se contém informações úteis para LLM
        useful_info = []
        if "SEXO" in result: useful_info.append("SEXO mappings")
        if "diagnóstico" in result.lower(): useful_info.append("diagnóstico info")
        if "município" in result.lower(): useful_info.append("município info")
        if "sample" in result.lower(): useful_info.append("sample data")
        if "internações" in result.lower(): useful_info.append("internações context")
        
        print(f"📝 Informações úteis para LLM: {useful_info}")
        
        # Verificar estrutura do output
        sections = []
        if "🏥" in result: sections.append("Hospital icons")
        if "📚" in result: sections.append("CID icons") 
        if "🌍" in result: sections.append("Geography icons")
        if "🎯" in result: sections.append("Selection guide")
        
        print(f"📑 Seções identificadas: {sections}")
        
        # 5. Testar como LLM veria o resultado  
        print(f"\n--- SIMULAÇÃO: Como LLM decide tabelas ---")
        
        queries_exemplo = [
            "Quantas internações existem?",
            "O que significa o código CID F190?", 
            "Qual cidade com mais casos de diabetes?",
            "Quantos homens foram internados?"
        ]
        
        # Usar o resultado já obtido da tool
        tool_output = result
        
        for query in queries_exemplo:
            print(f"\n🤔 Query: '{query}'")
            
            # Simular lógica de decisão (baseada em keywords)
            suggested_tables = []
            
            if any(word in query.lower() for word in ["internações", "internados", "casos", "homens", "mulheres"]):
                suggested_tables.append("internacoes")
            
            if any(word in query.lower() for word in ["cid", "código", "significa", "diagnóstico"]):
                suggested_tables.append("cid10")
                
            if any(word in query.lower() for word in ["cidade", "município", "região"]):
                suggested_tables.append("municipios")
            
            print(f"🎯 Tabelas sugeridas: {suggested_tables}")
            print(f"📋 Justificativa: Baseado em keywords da query e contexto da tool")
        
        return result
        
        print("\n🎉 TESTE DA ENHANCED LIST TABLES TOOL CONCLUÍDO!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO no teste: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_tool_integration_with_llm():
    """Testa integração da tool com LLM real"""
    
    print("\n=== TESTE INTEGRAÇÃO COM LLM REAL ===\n")
    
    try:
        # Configurar
        config = ApplicationConfig()
        llm_manager = HybridLLMManager(config)
        
        # Obter tool da lista de tools do manager
        tools = llm_manager.get_sql_tools()
        enhanced_tool = None
        
        for tool in tools:
            if tool.name == "sql_db_list_tables":
                enhanced_tool = tool
                break
        
        if not enhanced_tool:
            print("❌ EnhancedListTablesTool não encontrada nas tools do manager")
            return False
        
        print("✅ EnhancedListTablesTool encontrada no manager")
        print(f"📝 Descrição: {enhanced_tool.description[:100]}...")
        
        # Testar execução via LLM Manager
        print("\n🤖 Testando execução via LLM...")
        
        # Simular chamada da LLM
        result = enhanced_tool.invoke({})
        
        print("✅ Tool executada via LLM com sucesso!")
        print(f"📏 Resultado: {len(result)} caracteres")
        print(f"🎯 Contém 'internacoes': {'internacoes' in result}")
        print(f"🎯 Contém 'cid10': {'cid10' in result}")
        print(f"🎯 Contém 'municipios': {'municipios' in result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na integração: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Iniciando testes da EnhancedListTablesTool...")
    
    success1 = test_enhanced_list_tables_tool()
    success2 = test_tool_integration_with_llm()
    
    if success1 and success2:
        print("\n🎉 TODOS OS TESTES PASSARAM!")
        sys.exit(0)
    else:
        print("\n❌ ALGUNS TESTES FALHARAM!")
        sys.exit(1)