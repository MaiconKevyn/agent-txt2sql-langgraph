#!/usr/bin/env python3
"""
Teste de conexão PostgreSQL com HybridLLMManager
Validação da migração SQLite -> PostgreSQL
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.application.config.simple_config import ApplicationConfig
from src.langgraph_migration.llm_manager import HybridLLMManager

def test_postgresql_connection():
    """Testa conexão PostgreSQL com HybridLLMManager"""
    
    print("=== TESTE CONEXÃO POSTGRESQL ===\n")
    
    # Criar configuração para PostgreSQL
    config = ApplicationConfig()
    print(f"Database Type: {config.database_type}")
    print(f"Database Path: {config.database_path}")
    
    try:
        # Inicializar HybridLLMManager com PostgreSQL
        print("\n1. Inicializando HybridLLMManager...")
        llm_manager = HybridLLMManager(config)
        print("✅ HybridLLMManager inicializado com sucesso!")
        
        # Testar conexão SQL
        print("\n2. Testando conexão SQL Database...")
        sql_db = llm_manager.get_database()
        print("✅ SQL Database conectado!")
        
        # Listar tabelas
        print("\n3. Listando tabelas disponíveis...")
        tables = sql_db.get_usable_table_names()
        print(f"📊 Tabelas encontradas: {len(tables)}")
        for table in sorted(tables):
            print(f"  - {table}")
        
        # Testar schema de uma tabela
        print("\n4. Testando schema da tabela 'internacoes'...")
        if 'internacoes' in tables:
            schema = sql_db.get_table_info(['internacoes'])
            print("✅ Schema da tabela 'internacoes' obtido:")
            print(f"Primeiras 500 chars: {schema[:500]}...")
        else:
            print("❌ Tabela 'internacoes' não encontrada")
        
        # Testar query simples
        print("\n5. Testando query de contagem...")
        result = sql_db.run("SELECT COUNT(*) as total FROM internacoes LIMIT 1")
        print(f"✅ Total de registros em 'internacoes': {result}")
        
        # Testar toolkit
        print("\n6. Testando SQL Toolkit...")
        tools = llm_manager.get_sql_tools()
        print(f"✅ SQL Toolkit carregado com {len(tools)} tools:")
        for tool in tools:
            print(f"  - {tool.name}: {tool.description[:50]}...")
        
        print("\n🎉 CONEXÃO POSTGRESQL CONFIGURADA COM SUCESSO!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO na conexão PostgreSQL: {e}")
        print(f"Tipo do erro: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_postgresql_connection()
    sys.exit(0 if success else 1)