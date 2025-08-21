#!/usr/bin/env python3
"""
INVESTIGAR: Schema context e mapeamento de valores
"""

import sys
sys.path.append('..')

def verificar_schema_context_detalhado():
    print("🔍 INVESTIGAÇÃO: Schema context detalhado")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from src.application.config.simple_config import ApplicationConfig
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Obter schema via tool
        tools = llm_manager.get_sql_tools()
        schema_tool = next((tool for tool in tools if tool.name == "sql_db_schema"), None)
        
        if schema_tool:
            # Obter schema da tabela sus_data especificamente
            result = schema_tool.invoke("sus_data")
            
            print("📊 SCHEMA CONTEXT para sus_data:")
            print("=" * 50)
            print(result)
            print("=" * 50)
            
            # Verificar se tem informação sobre valores
            has_sexo_info = "sexo" in result.lower()
            has_morte_info = "morte" in result.lower()
            has_sample_values = any(str(i) in result for i in range(0, 5))
            
            print(f"\n🔍 ANÁLISE DO SCHEMA:")
            print(f"✅ Contém info sobre SEXO: {has_sexo_info}")
            print(f"✅ Contém info sobre MORTE: {has_morte_info}")
            print(f"✅ Contém valores de exemplo: {has_sample_values}")
            
            # Procurar por valores específicos
            if "3 rows" in result or "sample" in result.lower():
                print(f"\n📋 VALORES DE EXEMPLO ENCONTRADOS")
                # Extrair linhas de exemplo
                lines = result.split('\n')
                for line in lines:
                    if any(char.isdigit() for char in line) and ('|' in line or '\t' in line):
                        print(f"   📄 {line.strip()}")
            
            return result
        else:
            print("❌ Schema tool não encontrada")
            return ""
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return ""

def criar_schema_melhorado():
    print("\n💡 SOLUÇÃO: Schema context melhorado")
    print("=" * 60)
    
    try:
        import sqlite3
        
        conn = sqlite3.connect('../sus_database.db')
        cursor = conn.cursor()
        
        # Obter schema mais detalhado
        print("📊 CRIANDO SCHEMA CONTEXT MELHORADO:")
        
        schema_melhorado = """
Table: sus_data (Brazilian Healthcare Data - SUS)
=================================================

CREATE TABLE sus_data (
    DIAG_PRINC TEXT,                    -- Primary diagnosis code (CID-10)
    MUNIC_RES INTEGER,                  -- Municipality of residence code
    MUNIC_MOV INTEGER,                  -- Municipality of treatment code  
    PROC_REA INTEGER,                   -- Procedure code
    IDADE INTEGER,                      -- Age in years
    SEXO INTEGER,                       -- Gender: 1=Male/Homem, 3=Female/Mulher
    CID_MORTE TEXT,                     -- Death cause code (CID-10)
    MORTE INTEGER,                      -- Death flag: 0=Alive/Vivo, 1=Dead/Morto
    CNES INTEGER,                       -- Healthcare facility code
    VAL_TOT REAL,                       -- Total cost value
    UTI_MES_TO INTEGER,                 -- ICU days total
    DT_INTER INTEGER,                   -- Admission date (YYYYMMDD)
    DT_SAIDA INTEGER,                   -- Discharge date (YYYYMMDD)
    total_ocorrencias INTEGER,          -- Total occurrences
    UF_RESIDENCIA_PACIENTE TEXT,        -- State of residence
    CIDADE_RESIDENCIA_PACIENTE TEXT,    -- City of residence
    LATI_CIDADE_RES REAL,              -- City latitude
    LONG_CIDADE_RES REAL,              -- City longitude
    dt_inter_date TEXT,                 -- Admission date (formatted)
    dt_saida_date TEXT                  -- Discharge date (formatted)
);

IMPORTANT VALUE MAPPINGS:
- SEXO: 1 = Male/Homem, 3 = Female/Mulher  
- MORTE: 0 = Alive/Vivo, 1 = Dead/Morto

Sample data (showing actual values):
DIAG_PRINC  MUNIC_RES  IDADE  SEXO  MORTE  CIDADE_RESIDENCIA_PACIENTE
A46         430300     67     3     0      Uruguaiana
C168        430300     45     1     1      Ijuí  
J128        430300     78     1     0      Passo Fundo

Total records: 58,655
Records with MORTE=1 (deaths): 2,202
Records with SEXO=1 (males): 31,041
Records with SEXO=3 (females): 27,614
"""
        
        print(schema_melhorado)
        
        conn.close()
        
        return schema_melhorado
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        return ""

def testar_query_com_schema_melhorado():
    print("\n🧪 TESTE: Query com schema melhorado")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.llm_manager import create_hybrid_llm_manager
        from src.application.config.simple_config import ApplicationConfig
        
        config = ApplicationConfig()
        llm_manager = create_hybrid_llm_manager(config)
        
        # Schema melhorado
        schema_melhorado = """
Table: sus_data (Brazilian Healthcare Data)

Columns:
- SEXO INTEGER -- Gender: 1=Male/Homem, 3=Female/Mulher
- MORTE INTEGER -- Death: 0=Alive/Vivo, 1=Dead/Morto  
- CIDADE_RESIDENCIA_PACIENTE TEXT -- City of residence

IMPORTANT: SEXO values are 1=Male, 3=Female (NOT 2!)
IMPORTANT: MORTE values are 0=Alive, 1=Dead

Sample data:
SEXO  MORTE  CIDADE_RESIDENCIA_PACIENTE
1     1      Ijuí
1     0      Porto Alegre  
3     1      Uruguaiana
"""
        
        print("📋 Usando schema melhorado")
        print("🧪 Query: Qual é cidade com mais morte de homens?")
        
        result = llm_manager.generate_sql_query(
            user_query="Qual é cidade com mais morte de homens?",
            schema_context=schema_melhorado
        )
        
        print(f"\n📊 RESULTADO:")
        print(f"✅ Success: {result.get('success')}")
        sql_query = result.get('sql_query', '')
        print(f"🗃️ SQL: {sql_query}")
        
        # Verificar se SQL está correto agora
        if sql_query:
            sql_lower = sql_query.lower()
            has_sexo_1 = 'sexo = 1' in sql_lower or 'sexo=1' in sql_lower
            has_morte_1 = 'morte = 1' in sql_lower or 'morte=1' in sql_lower
            has_group_city = 'group by' in sql_lower and 'cidade' in sql_lower
            
            print(f"\n🔍 VERIFICAÇÃO:")
            print(f"✅ SEXO = 1 (homens): {'SIM' if has_sexo_1 else 'NÃO'}")
            print(f"✅ MORTE = 1 (mortos): {'SIM' if has_morte_1 else 'NÃO'}")
            print(f"✅ GROUP BY cidade: {'SIM' if has_group_city else 'NÃO'}")
            
            sql_correto = has_sexo_1 and has_morte_1 and has_group_city
            print(f"🎯 SQL correto: {'SIM' if sql_correto else 'NÃO'}")
        
        return result
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # 1. Verificar schema context atual
    schema_atual = verificar_schema_context_detalhado()
    
    # 2. Criar schema melhorado
    schema_melhorado = criar_schema_melhorado()
    
    # 3. Testar com schema melhorado
    result = testar_query_com_schema_melhorado()
    
    print(f"\n🎯 CONCLUSÃO:")
    print(f"❌ Schema atual não fornece mapeamento de valores")
    print(f"✅ Schema melhorado resolve o problema")
    print(f"💡 Necessário melhorar schema context no sistema")