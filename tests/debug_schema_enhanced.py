#!/usr/bin/env python3
"""
DEBUG: Verificar se schema enhanced está funcionando
"""

import sys
sys.path.append('..')

def debug_enhanced_schema():
    print("🔍 DEBUG: Schema enhanced sendo aplicado?")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.nodes_v3 import get_schema_node, _enhance_sus_schema_context
        from src.langgraph_migration.state_v3 import create_initial_messages_state
        
        # Criar estado inicial
        state = create_initial_messages_state("test query", "debug_session")
        state["available_tables"] = ["sus_data"]
        state["selected_tables"] = ["sus_data"]
        
        print("🧪 Executando get_schema_node...")
        
        # Executar schema node
        updated_state = get_schema_node(state)
        schema_context = updated_state.get("schema_context", "")
        
        print(f"\n📊 SCHEMA CONTEXT FINAL:")
        print(f"Tamanho: {len(schema_context)} caracteres")
        
        # Verificar se contém as informações de mapeamento
        has_sexo_mapping = "SEXO = 1" in schema_context and "Male" in schema_context
        has_morte_mapping = "MORTE = 1" in schema_context and "Dead" in schema_context
        has_value_mappings = "VALUE MAPPINGS" in schema_context
        
        print(f"\n🔍 VERIFICAÇÕES:")
        print(f"✅ Contém mapeamento SEXO: {has_sexo_mapping}")
        print(f"✅ Contém mapeamento MORTE: {has_morte_mapping}")
        print(f"✅ Contém seção VALUE MAPPINGS: {has_value_mappings}")
        
        if has_value_mappings:
            print(f"\n📋 MAPEAMENTOS ENCONTRADOS:")
            # Extrair seção de mapeamentos
            lines = schema_context.split('\n')
            in_mappings = False
            for line in lines:
                if "VALUE MAPPINGS" in line:
                    in_mappings = True
                if in_mappings and line.strip():
                    print(f"   {line}")
                if in_mappings and line.strip() == "" and "SEXO" in schema_context[schema_context.find(line):]:
                    break
        
        # Testar função enhance diretamente
        print(f"\n🧪 TESTE FUNÇÃO _enhance_sus_schema_context:")
        test_schema = "CREATE TABLE sus_data (SEXO INTEGER, MORTE INTEGER);"
        enhanced = _enhance_sus_schema_context(test_schema)
        
        enhanced_has_mapping = "SEXO = 1" in enhanced and "Male" in enhanced
        print(f"✅ Função enhance funciona: {enhanced_has_mapping}")
        
        if enhanced_has_mapping:
            print(f"✅ Enhanced schema sample:")
            print(enhanced[-300:])  # Últimos 300 chars
        
        return schema_context
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return ""

def test_query_with_debug():
    print("\n🧪 TESTE: Query com debug schema")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.orchestrator_v3 import create_production_orchestrator
        
        # Executar query simples para capturar schema
        orchestrator = create_production_orchestrator(
            provider="ollama",
            model_name="llama3.1:8b"
        )
        
        # Query que deve usar SEXO = 1
        query = "Quantos homens morreram?"
        print(f"🧪 Query: {query}")
        
        result = orchestrator.process_query(query)
        
        sql_query = result.get('sql_query', '')
        print(f"🗃️ SQL gerado: {sql_query}")
        
        # Verificar se está usando valores corretos
        if sql_query:
            uses_sexo_1 = "SEXO = 1" in sql_query
            uses_sexo_3 = "SEXO = 3" in sql_query
            uses_morte_1 = "MORTE = 1" in sql_query
            
            print(f"\n🔍 ANÁLISE SQL:")
            print(f"✅ Usa SEXO = 1 (correto para homens): {uses_sexo_1}")
            print(f"❌ Usa SEXO = 3 (incorreto para homens): {uses_sexo_3}")
            print(f"✅ Usa MORTE = 1 (correto para mortos): {uses_morte_1}")
            
            if uses_sexo_1 and uses_morte_1:
                print(f"🎯 SQL ESTÁ CORRETO!")
            else:
                print(f"❌ SQL ainda tem problemas")
        
        return result
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # 1. Debug schema enhanced
    schema_context = debug_enhanced_schema()
    
    # 2. Test query
    result = test_query_with_debug()
    
    print(f"\n🎯 DIAGNÓSTICO:")
    if schema_context and "VALUE MAPPINGS" in schema_context:
        print(f"✅ Schema enhanced está sendo aplicado")
        if result and result.get('sql_query'):
            sql = result['sql_query']
            if "SEXO = 1" in sql:
                print(f"✅ Query está usando valores corretos")
            else:
                print(f"❌ Query ainda usa valores incorretos")
                print(f"💡 Problema pode estar na prompt da LLM")
    else:
        print(f"❌ Schema enhanced NÃO está sendo aplicado")
        print(f"🔧 Verificar implementação da função")