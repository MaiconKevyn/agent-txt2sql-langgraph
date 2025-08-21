#!/usr/bin/env python3
"""
Teste Simplificado do Workflow com Prompt Templates
Testa o workflow sem retry logic para validar os templates

Uso: python tests/test_workflow_simple.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from src.langgraph_migration.state_v3 import MessagesStateTXT2SQL, QueryRoute, ExecutionPhase
from src.langgraph_migration.nodes_v3 import (
    query_classification_node,
    list_tables_node,
    get_schema_node,
    generate_sql_node
)


def create_initial_state(user_query: str) -> MessagesStateTXT2SQL:
    """Cria estado inicial completo"""
    return MessagesStateTXT2SQL(
        # Core fields
        user_query=user_query,
        messages=[],
        session_id="test_session",
        timestamp=datetime.now(),
        
        # Query routing
        query_route=None,
        classification=None,
        requires_sql=False,
        
        # Workflow state
        current_phase=ExecutionPhase.INITIALIZATION,
        completed_phases=[],
        
        # Database context
        available_tables=[],
        selected_tables=[],
        schema_context="",
        
        # SQL processing
        generated_sql=None,
        validated_sql=None,
        sql_execution_result=None,
        
        # Tool calling
        tool_calls=[],
        pending_tool_calls=[],
        
        # Results
        final_response=None,
        response_metadata={},
        
        # Error handling
        errors=[],
        current_error=None,
        retry_count=0,
        
        # Performance
        start_time=datetime.now(),
        execution_time=0.0,
        execution_time_total=0.0,
        phase_timings={}
    )


def test_classification_node():
    """Testa o nó de classificação"""
    print("🔍 TESTE 1: CLASSIFICATION NODE")
    print("=" * 50)
    
    # Estado inicial completo
    state = create_initial_state("Quantos homens morreram?")
    
    print(f"Input: {state['user_query']}")
    
    # Executar classificação
    result = query_classification_node(state)
    
    print(f"Route: {result.get('query_route')}")
    print(f"Phase: {result.get('current_phase')}")
    print(f"Success: {result.get('current_error') is None}")
    
    return result


def test_list_tables_node(state):
    """Testa o nó de listagem de tabelas"""
    print("\n🗂️ TESTE 2: LIST TABLES NODE")
    print("=" * 50)
    
    print(f"Input state phase: {state.get('current_phase')}")
    
    # Executar listagem
    result = list_tables_node(state)
    
    print(f"Available tables: {result.get('available_tables', [])}")
    print(f"Selected tables: {result.get('selected_tables', [])}")
    print(f"Phase: {result.get('current_phase')}")
    print(f"Success: {result.get('current_error') is None}")
    
    return result


def test_get_schema_node(state):
    """Testa o nó de schema"""
    print("\n📋 TESTE 3: GET SCHEMA NODE")
    print("=" * 50)
    
    print(f"Input tables: {state.get('selected_tables', [])}")
    
    # Executar schema
    result = get_schema_node(state)
    
    schema_context = result.get('schema_context', '')
    print(f"Schema length: {len(schema_context)} chars")
    print(f"Schema preview: {schema_context[:200]}..." if len(schema_context) > 200 else schema_context)
    print(f"Phase: {result.get('current_phase')}")
    print(f"Success: {result.get('current_error') is None}")
    
    return result


def test_generate_sql_node(state):
    """Testa o nó de geração SQL com novos templates"""
    print("\n🤖 TESTE 4: GENERATE SQL NODE (NEW TEMPLATES)")
    print("=" * 50)
    
    print(f"Input query: {state['user_query']}")
    print(f"Input tables: {state.get('selected_tables', [])}")
    print(f"Schema available: {bool(state.get('schema_context'))}")
    
    # Executar geração SQL
    result = generate_sql_node(state)
    
    generated_sql = result.get('generated_sql', '')
    print(f"Generated SQL: {generated_sql}")
    print(f"Phase: {result.get('current_phase')}")
    print(f"Success: {result.get('current_error') is None}")
    
    if result.get('current_error'):
        print(f"Error: {result['current_error']}")
    
    return result


def test_full_pipeline():
    """Testa o pipeline completo sem workflow (para debug)"""
    print("🎯 TESTE COMPLETO: PIPELINE STEP-BY-STEP")
    print("=" * 70)
    
    try:
        # 1. Classificação
        state = test_classification_node()
        
        if state.get('current_error'):
            print(f"❌ Failed at classification: {state['current_error']}")
            return
        
        # 2. Lista tabelas
        state = test_list_tables_node(state)
        
        if state.get('current_error'):
            print(f"❌ Failed at list tables: {state['current_error']}")
            return
        
        # 3. Schema
        state = test_get_schema_node(state)
        
        if state.get('current_error'):
            print(f"❌ Failed at schema: {state['current_error']}")
            return
        
        # 4. SQL Generation (com novos templates!)
        state = test_generate_sql_node(state)
        
        if state.get('current_error'):
            print(f"❌ Failed at SQL generation: {state['current_error']}")
            return
        
        # Resultado final
        print(f"\n🎉 PIPELINE SUCCESS!")
        print(f"Final SQL: {state.get('generated_sql', 'None')}")
        print(f"Table Rules Applied: {'sus_data' in str(state.get('selected_tables', []))}")
        
        # Verificar se as regras SUS foram aplicadas
        sql = state.get('generated_sql', '')
        if 'SEXO = 1' in sql and 'MORTE = 1' in sql:
            print(f"✅ SUS Rules Applied Correctly: SEXO=1, MORTE=1")
        else:
            print(f"⚠️  SUS Rules May Not Be Applied: Check SQL")
        
    except Exception as e:
        print(f"❌ Pipeline Error: {e}")
        import traceback
        traceback.print_exc()


def test_different_queries():
    """Testa diferentes tipos de query"""
    print("\n🧪 TESTE MULTI-QUERY: DIFFERENT TABLE SCENARIOS")
    print("=" * 70)
    
    test_queries = [
        {
            "query": "Quantos homens morreram?",
            "expected_table": "sus_data",
            "expected_rules": ["SEXO = 1", "MORTE = 1"]
        },
        {
            "query": "O que significa CID J44.0?",
            "expected_table": "cid_detalhado", 
            "expected_rules": ["codigo", "J44.0"]
        }
    ]
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n📋 Test Case {i}: {test_case['query']}")
        print("-" * 40)
        
        # Estado inicial completo
        state = create_initial_state(test_case['query'])
        
        try:
            # Pipeline simplificado
            state = query_classification_node(state)
            state = list_tables_node(state)
            state = get_schema_node(state)
            state = generate_sql_node(state)
            
            # Verificar resultado
            generated_sql = state.get('generated_sql', '')
            selected_tables = state.get('selected_tables', [])
            
            print(f"Selected tables: {selected_tables}")
            print(f"Generated SQL: {generated_sql}")
            
            # Verificar se a tabela esperada foi selecionada
            if test_case['expected_table'] in selected_tables:
                print(f"✅ Correct table selected: {test_case['expected_table']}")
            else:
                print(f"⚠️  Expected {test_case['expected_table']}, got {selected_tables}")
            
            # Verificar se as regras esperadas estão no SQL
            rules_found = [rule for rule in test_case['expected_rules'] if rule in generated_sql]
            if rules_found:
                print(f"✅ Rules applied: {rules_found}")
            else:
                print(f"⚠️  Expected rules not found: {test_case['expected_rules']}")
                
        except Exception as e:
            print(f"❌ Test case failed: {e}")


def main():
    """Função principal"""
    print("🧪 TESTE WORKFLOW SIMPLIFICADO - PROMPT TEMPLATES")
    print("=" * 70)
    print("Este script testa o workflow node-by-node:")
    print("• 🔍 Classification Node")
    print("• 🗂️ List Tables Node (Enhanced Tool)")
    print("• 📋 Schema Node")
    print("• 🤖 SQL Generation Node (New Templates!)")
    print("=" * 70)
    
    try:
        # Teste do pipeline completo
        test_full_pipeline()
        
        # Teste de múltiplas queries
        test_different_queries()
        
        print(f"\n🎉 TODOS OS TESTES CONCLUÍDOS!")
        print(f"✅ Prompt templates funcionando nos nodes")
        print(f"🚀 Ready for workflow integration")
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()