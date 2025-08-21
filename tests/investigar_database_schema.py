#!/usr/bin/env python3
"""
INVESTIGAÇÃO: Schema das tabelas e contexto para a LLM
"""

import sys
import sqlite3
sys.path.append('..')

def verificar_database_atual():
    print("🗃️ INVESTIGAÇÃO: Qual database está sendo usado")
    print("=" * 60)
    
    try:
        from src.application.config.simple_config import ApplicationConfig
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        
        # Verificar config
        config = ApplicationConfig()
        print(f"📋 Database configurado: {config.database_path}")
        
        # Verificar LLM manager
        llm_manager = create_hybrid_llm_manager(config)
        database = llm_manager.get_database()
        
        print(f"📊 SQLDatabase URI: {database._engine.url}")
        print(f"📊 Tipo do database: {type(database)}")
        
        # Listar tabelas disponíveis
        table_names = database.get_usable_table_names()
        print(f"\n📋 TABELAS DISPONÍVEIS ({len(table_names)}):")
        for i, table in enumerate(table_names, 1):
            print(f"   {i}. {table}")
        
        return database, table_names
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None, []

def verificar_database_direto():
    print("\n🔍 VERIFICAÇÃO DIRETA: sus_database.db")
    print("=" * 60)
    
    try:
        # Conectar diretamente ao arquivo
        conn = sqlite3.connect('../sus_database.db')
        cursor = conn.cursor()
        
        # Obter lista de tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print(f"📋 TABELAS NO ARQUIVO ({len(tables)}):")
        for i, (table_name,) in enumerate(tables, 1):
            print(f"   {i}. {table_name}")
            
            # Obter schema de cada tabela
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            print(f"      📊 Colunas ({len(columns)}):")
            for col in columns:
                col_name, col_type = col[1], col[2]
                print(f"         - {col_name} ({col_type})")
            
            # Contar registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"      📈 Registros: {count}")
            print()
        
        conn.close()
        return tables
        
    except Exception as e:
        print(f"❌ Erro ao conectar diretamente: {e}")
        return []

def investigar_como_schema_e_usado():
    print("🧠 INVESTIGAÇÃO: Como o schema é fornecido à LLM")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from src.application.config.simple_config import ApplicationConfig
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Simular obtenção de schema como no workflow
        database = llm_manager.get_database()
        
        # Método 1: Obter schema de todas as tabelas
        print("📋 MÉTODO 1: Get table info (como no workflow)")
        table_names = database.get_usable_table_names()
        
        for table in table_names[:2]:  # Primeiras 2 tabelas
            print(f"\n🔍 TABELA: {table}")
            
            # Como o workflow obtém schema
            table_info = database.get_table_info([table])
            print(f"📊 Table info: {table_info[:200]}...")
        
        # Método 2: Usar tool sql_db_schema diretamente
        print(f"\n📋 MÉTODO 2: Usando tool sql_db_schema")
        tools = llm_manager.get_sql_tools()
        schema_tool = next((tool for tool in tools if tool.name == "sql_db_schema"), None)
        
        if schema_tool:
            # Testar tool
            sample_table = table_names[0] if table_names else "sus_data"
            result = schema_tool.invoke(sample_table)
            print(f"🔧 Tool result para '{sample_table}':")
            print(f"   {result[:300]}...")
        
        return table_names
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return []

def verificar_workflow_schema_usage():
    print("\n🔄 INVESTIGAÇÃO: Como o workflow usa schema")
    print("=" * 60)
    
    try:
        # Simular uma query real para ver o contexto
        from src.langgraph_migration.orchestrator_v3 import create_production_orchestrator
        
        orchestrator = create_production_orchestrator(
            provider="ollama",
            model_name="llama3.1:8b"
        )
        
        # Query simples para capturar o schema context
        print("🧪 Executando query para capturar schema context...")
        
        # Vamos interceptar o processo para ver o schema context
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from src.application.config.simple_config import ApplicationConfig
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Simular o que acontece no workflow
        database = llm_manager.get_database()
        
        # 1. List tables (como no workflow)
        tables = database.get_usable_table_names()
        print(f"📋 Tabelas descobertas: {tables}")
        
        # 2. Get schema context (como no workflow)
        schema_context = ""
        for table in tables:
            table_schema = database.get_table_info([table])
            schema_context += f"\nTable: {table}\n{table_schema}\n"
        
        print(f"\n📊 SCHEMA CONTEXT GERADO:")
        print(f"Tamanho: {len(schema_context)} caracteres")
        print(f"Conteúdo (sample):")
        print(schema_context[:500] + "..." if len(schema_context) > 500 else schema_context)
        
        return schema_context
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return ""

def comparar_expectativa_vs_realidade():
    print("\n⚖️ COMPARAÇÃO: Expectativa vs Realidade")
    print("=" * 60)
    
    print("🎯 EXPECTATIVA (que você mencionou):")
    print("   1. LLM deveria receber schema completo de cada tabela")
    print("   2. Schema como contexto para gerar SQL")
    print("   3. Usar sus_database.db como fonte")
    
    print("\n🔍 REALIDADE (o que encontramos):")
    
    # Verificar database
    try:
        from src.application.config.simple_config import ApplicationConfig
        config = ApplicationConfig()
        print(f"   ✅ Database configurado: {config.database_path}")
        
        # Verificar se arquivo existe
        import os
        if os.path.exists(config.database_path):
            print(f"   ✅ Arquivo existe: {config.database_path}")
        else:
            print(f"   ❌ Arquivo NÃO existe: {config.database_path}")
            
    except Exception as e:
        print(f"   ❌ Erro na verificação: {e}")

if __name__ == "__main__":
    # 1. Verificar database atual
    database, tables = verificar_database_atual()
    
    # 2. Verificar database direto
    direct_tables = verificar_database_direto()
    
    # 3. Como schema é usado
    workflow_tables = investigar_como_schema_e_usado()
    
    # 4. Workflow schema usage  
    schema_context = verificar_workflow_schema_usage()
    
    # 5. Comparação
    comparar_expectativa_vs_realidade()
    
    print(f"\n🎯 RESUMO:")
    print(f"📋 Tabelas via LangChain: {len(tables) if tables else 0}")
    print(f"📋 Tabelas via SQLite direto: {len(direct_tables)}")
    print(f"📊 Schema context gerado: {len(schema_context) if schema_context else 0} chars")