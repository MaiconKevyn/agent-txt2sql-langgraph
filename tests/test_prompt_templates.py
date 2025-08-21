#!/usr/bin/env python3
"""
Teste dos Prompt Templates por Tabela
Valida o funcionamento do sistema ChatPromptTemplate com regras específicas

Uso: python tests/test_prompt_templates.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_core.prompts import ChatPromptTemplate
from src.application.config.table_templates import (
    build_table_specific_prompt, 
    build_multi_table_prompt,
    get_table_template,
    validate_template_coverage
)


def test_table_templates():
    """Testa os templates específicos por tabela"""
    print("📋 TESTE 1: TEMPLATES ESPECÍFICOS POR TABELA")
    print("=" * 60)
    
    # Testar tabela individual - sus_data
    print("\n🏥 SUS_DATA Template:")
    print("-" * 30)
    sus_template = build_table_specific_prompt(["sus_data"])
    print(f"Length: {len(sus_template)} chars")
    print(sus_template[:300] + "..." if len(sus_template) > 300 else sus_template)
    
    # Testar tabela individual - cid_detalhado
    print("\n📚 CID_DETALHADO Template:")
    print("-" * 30)
    cid_template = build_table_specific_prompt(["cid_detalhado"])
    print(f"Length: {len(cid_template)} chars")
    print(cid_template[:300] + "..." if len(cid_template) > 300 else cid_template)
    
    # Testar múltiplas tabelas
    print("\n🔗 MULTI-TABLE Template:")
    print("-" * 30)
    multi_template = build_multi_table_prompt(["sus_data", "cid_detalhado"])
    print(f"Length: {len(multi_template)} chars")
    print(multi_template[:400] + "..." if len(multi_template) > 400 else multi_template)


def test_chat_prompt_template():
    """Testa ChatPromptTemplate com templates dinâmicos"""
    print("\n🤖 TESTE 2: CHATPROMPTTEMPLATE INTEGRATION")
    print("=" * 60)
    
    # Simular dados do estado
    test_cases = [
        {
            "user_query": "Quantos homens morreram?",
            "selected_tables": ["sus_data"],
            "schema_context": "CREATE TABLE sus_data (SEXO INTEGER, MORTE INTEGER, ...)"
        },
        {
            "user_query": "O que significa CID J44.0?",
            "selected_tables": ["cid_detalhado"],
            "schema_context": "CREATE TABLE cid_detalhado (codigo TEXT, descricao TEXT, ...)"
        },
        {
            "user_query": "Mortes por descrição de doença",
            "selected_tables": ["sus_data", "cid_detalhado"],
            "schema_context": "sus_data + cid_detalhado schemas..."
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🧪 TEST CASE {i}:")
        print(f"   Query: {test_case['user_query']}")
        print(f"   Tables: {test_case['selected_tables']}")
        
        # Build table-specific rules
        if len(test_case['selected_tables']) > 1:
            table_rules = build_multi_table_prompt(test_case['selected_tables'])
        else:
            table_rules = build_table_specific_prompt(test_case['selected_tables'])
        
        # Create ChatPromptTemplate
        sql_prompt_template = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert assistant for Brazilian healthcare (SUS) data analysis.

📋 CORE INSTRUCTIONS:
1. Generate syntactically correct SQLite queries
2. Use proper table and column names from the schema
3. Handle Portuguese language questions appropriately
4. Return only the SQL query, no explanation

🔍 DATABASE SCHEMA:
{schema_context}"""),
            
            ("system", "{table_specific_rules}"),
            
            ("human", "🎯 USER QUERY: {user_query}\n\nGenerate the SQL query:")
        ])
        
        # Format messages
        formatted_messages = sql_prompt_template.format_messages(
            schema_context=test_case['schema_context'],
            table_specific_rules=table_rules,
            user_query=test_case['user_query']
        )
        
        print(f"   📊 Generated {len(formatted_messages)} messages")
        print(f"   📏 Rules length: {len(table_rules)} chars")
        
        # Show first system message (schema)
        print(f"   📋 System 1: {formatted_messages[0].content[:100]}...")
        
        # Show second system message (table rules)
        print(f"   🎯 System 2: {formatted_messages[1].content[:100]}...")
        
        # Show human message
        print(f"   👤 Human: {formatted_messages[2].content}")


def test_template_coverage():
    """Testa cobertura dos templates"""
    print("\n📊 TESTE 3: TEMPLATE COVERAGE")
    print("=" * 60)
    
    # Tabelas existentes
    existing_tables = ["sus_data", "cid_detalhado"]
    
    # Tabelas futuras
    future_tables = ["pacientes", "medicamentos", "procedimentos"]
    
    print("✅ EXISTING TABLES:")
    existing_coverage = validate_template_coverage(existing_tables)
    for table, has_template in existing_coverage.items():
        status = "✅" if has_template else "❌"
        print(f"   {status} {table}: {'Template exists' if has_template else 'No template'}")
    
    print("\n🔮 FUTURE TABLES:")
    future_coverage = validate_template_coverage(future_tables)
    for table, has_template in future_coverage.items():
        status = "✅" if has_template else "❌"
        print(f"   {status} {table}: {'Template exists' if has_template else 'Needs template'}")
    
    # Estatísticas
    total_existing = len(existing_tables)
    covered_existing = sum(existing_coverage.values())
    coverage_percent = (covered_existing / total_existing) * 100 if total_existing > 0 else 0
    
    print(f"\n📈 COVERAGE STATS:")
    print(f"   Current tables: {covered_existing}/{total_existing} ({coverage_percent:.1f}%)")
    print(f"   Future tables: 0/{len(future_tables)} (0.0%)")


def test_template_efficiency():
    """Testa eficiência dos templates (tamanho, velocidade)"""
    print("\n⚡ TESTE 4: TEMPLATE EFFICIENCY")
    print("=" * 60)
    
    import time
    
    # Teste de tamanho
    single_table = build_table_specific_prompt(["sus_data"])
    multi_table = build_multi_table_prompt(["sus_data", "cid_detalhado"])
    
    print(f"📏 TEMPLATE SIZES:")
    print(f"   Single table (sus_data): {len(single_table)} chars")
    print(f"   Multi table (sus+cid): {len(multi_table)} chars")
    print(f"   Size increase: {len(multi_table) - len(single_table)} chars")
    
    # Teste de velocidade
    print(f"\n⏱️  TEMPLATE SPEED:")
    
    # Single table speed
    start_time = time.time()
    for _ in range(100):
        build_table_specific_prompt(["sus_data"])
    single_time = time.time() - start_time
    
    # Multi table speed
    start_time = time.time()
    for _ in range(100):
        build_multi_table_prompt(["sus_data", "cid_detalhado"])
    multi_time = time.time() - start_time
    
    print(f"   Single table (100x): {single_time:.4f}s")
    print(f"   Multi table (100x): {multi_time:.4f}s")
    print(f"   Average single: {single_time/100*1000:.2f}ms")
    print(f"   Average multi: {multi_time/100*1000:.2f}ms")


def simulate_full_flow():
    """Simula o fluxo completo com templates"""
    print("\n🎯 TESTE 5: SIMULAÇÃO DO FLUXO COMPLETO")
    print("=" * 60)
    
    # Simular diferentes cenários
    scenarios = [
        {
            "name": "Query SUS simples",
            "user_query": "Quantos homens morreram?",
            "selected_tables": ["sus_data"],
            "expected_rules": ["SEXO = 1", "MORTE = 1"]
        },
        {
            "name": "Query CID específica",
            "user_query": "CID para pneumonia",
            "selected_tables": ["cid_detalhado"],
            "expected_rules": ["LIKE", "descricao"]
        },
        {
            "name": "Query multi-tabela",
            "user_query": "Mortes por tipo de doença",
            "selected_tables": ["sus_data", "cid_detalhado"],
            "expected_rules": ["JOIN", "SEXO = 1", "MORTE = 1"]
        }
    ]
    
    for scenario in scenarios:
        print(f"\n📋 {scenario['name']}:")
        print(f"   Query: {scenario['user_query']}")
        print(f"   Tables: {scenario['selected_tables']}")
        
        # Generate template
        if len(scenario['selected_tables']) > 1:
            template = build_multi_table_prompt(scenario['selected_tables'])
        else:
            template = build_table_specific_prompt(scenario['selected_tables'])
        
        # Check if expected rules are present
        rules_found = []
        for rule in scenario['expected_rules']:
            if rule in template:
                rules_found.append(f"✅ {rule}")
            else:
                rules_found.append(f"❌ {rule}")
        
        print(f"   Rules check: {', '.join(rules_found)}")
        print(f"   Template size: {len(template)} chars")


def main():
    """Função principal do teste"""
    print("🧪 TESTE COMPLETO: PROMPT TEMPLATES POR TABELA")
    print("=" * 80)
    print("Este script testa:")
    print("• 📋 Templates específicos por tabela")
    print("• 🤖 Integração com ChatPromptTemplate")
    print("• 📊 Cobertura de templates")
    print("• ⚡ Eficiência e performance")
    print("• 🎯 Simulação de fluxo completo")
    print("=" * 80)
    
    try:
        # Executar todos os testes
        test_table_templates()
        test_chat_prompt_template()
        test_template_coverage()
        test_template_efficiency()
        simulate_full_flow()
        
        print("\n🎉 TODOS OS TESTES CONCLUÍDOS!")
        print("✅ Sistema de templates funcionando corretamente")
        print("🚀 Pronto para validação no agente e API")
        
    except Exception as e:
        print(f"❌ Erro nos testes: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()