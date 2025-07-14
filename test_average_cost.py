#!/usr/bin/env python3
"""
Test script to validate the average cost per city query
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_average_cost_query():
    """Test the average cost per city query"""
    
    # Create configuration
    app_config = ApplicationConfig(
        database_path="sus_database.db",
        llm_provider="ollama",
        llm_model="mistral",
        llm_temperature=0.0
    )
    
    orchestrator_config = OrchestratorConfig()
    
    # Create orchestrator
    orchestrator = Text2SQLOrchestrator(app_config, orchestrator_config)
    
    print("🔍 TESTE: 'Qual o gasto médio por atendimento em cada cidade?'")
    print("=" * 70)
    
    # Process the query
    result = orchestrator.process_single_query("Qual o gasto médio por atendimento em cada cidade?")
    
    print(f"✅ SUCCESS: {result.success}")
    print(f"🔧 SQL: {result.sql_query}")
    print(f"📊 RESULTS (first 5): {result.results[:5] if result.results else []}")
    print(f"⏱️ TIME: {result.execution_time:.2f}s")
    
    if result.success and result.results:
        print("\n" + "="*70)
        print("🏆 PRIMEIRAS 5 CIDADES COM GASTO:")
        for i, row in enumerate(result.results[:5], 1):
            cidade = row.get('cidade', row.get('CIDADE_RESIDENCIA_PACIENTE', 'N/A'))
            custo = row.get('custo_medio', row.get('custo_total', row.get('AVG(VAL_TOT)', 'N/A')))
            if custo != 'N/A':
                print(f"{i}. {cidade}: R$ {float(custo):.2f}")
            else:
                print(f"{i}. {cidade}: {custo}")
        print("="*70)
    else:
        print(f"❌ ERROR: {result.error_message}")
    
    # Test different scenarios
    print(f"\n🔧 TESTES MANUAIS PARA COMPARAÇÃO:")
    
    # Test 1: With deaths included
    sql_with_deaths = """
    SELECT CIDADE_RESIDENCIA_PACIENTE AS cidade, 
           AVG(VAL_TOT) AS custo_medio 
    FROM sus_data 
    GROUP BY CIDADE_RESIDENCIA_PACIENTE 
    ORDER BY custo_medio DESC 
    LIMIT 3;
    """
    
    result_with_deaths = orchestrator._query_service.execute_sql_query(sql_with_deaths)
    if result_with_deaths.success:
        print("🔴 COM MORTES INCLUÍDAS (TOP 3):")
        for row in result_with_deaths.results:
            print(f"  {row['cidade']}: R$ {row['custo_medio']:.2f}")
    
    # Test 2: Without deaths
    sql_without_deaths = """
    SELECT CIDADE_RESIDENCIA_PACIENTE AS cidade, 
           AVG(VAL_TOT) AS custo_medio 
    FROM sus_data 
    WHERE MORTE = 0
    GROUP BY CIDADE_RESIDENCIA_PACIENTE 
    ORDER BY custo_medio DESC 
    LIMIT 3;
    """
    
    result_without_deaths = orchestrator._query_service.execute_sql_query(sql_without_deaths)
    if result_without_deaths.success:
        print("🟢 SEM MORTES (TOP 3):")
        for row in result_without_deaths.results:
            print(f"  {row['cidade']}: R$ {row['custo_medio']:.2f}")
    
    # Test 3: Check cost distribution by death status
    sql_death_stats = """
    SELECT 
        CASE WHEN MORTE = 1 THEN 'Com morte' ELSE 'Sem morte' END as status,
        COUNT(*) as casos,
        AVG(VAL_TOT) as custo_medio,
        MIN(VAL_TOT) as custo_min,
        MAX(VAL_TOT) as custo_max
    FROM sus_data 
    GROUP BY MORTE;
    """
    
    result_death_stats = orchestrator._query_service.execute_sql_query(sql_death_stats)
    if result_death_stats.success:
        print("\n📊 ESTATÍSTICAS POR STATUS DE MORTE:")
        for row in result_death_stats.results:
            print(f"  {row['status']}: {row['casos']} casos, R$ {row['custo_medio']:.2f} médio")
    
    # Analysis
    print(f"\n🤔 ANÁLISE:")
    print(f"Por que o LLM está excluindo mortes?")
    print(f"1. Pode estar interpretando 'atendimento' como 'tratamento bem-sucedido'")
    print(f"2. Pode assumir que mortes têm custos diferentes (internações longas vs. curtas)")
    print(f"3. Pode estar seguindo alguma lógica médica/administrativa")
    
    # Check if there's a significant difference
    if result_with_deaths.success and result_without_deaths.success:
        avg_with = sum(row['custo_medio'] for row in result_with_deaths.results) / len(result_with_deaths.results)
        avg_without = sum(row['custo_medio'] for row in result_without_deaths.results) / len(result_without_deaths.results)
        difference = ((avg_without - avg_with) / avg_with) * 100
        print(f"\n📈 DIFERENÇA MÉDIA: {difference:+.1f}% ({avg_with:.2f} → {avg_without:.2f})")

if __name__ == "__main__":
    test_average_cost_query()