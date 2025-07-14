#!/usr/bin/env python3
"""
Test script to validate the male deaths query
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_male_deaths_query():
    """Test the male deaths query"""
    
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
    
    print("🔍 TESTE: 'Qual é o município com o maior numero de obitos masculino?'")
    print("=" * 70)
    
    # Process the query
    result = orchestrator.process_single_query("Qual é o município com o maior numero de obitos masculino?")
    
    print(f"✅ SUCCESS: {result.success}")
    print(f"🔧 SQL: {result.sql_query}")
    print(f"📊 RESULTS: {result.results}")
    print(f"⏱️ TIME: {result.execution_time:.2f}s")
    
    if result.success and result.results:
        municipio = result.results[0].get('CIDADE_RESIDENCIA_PACIENTE', result.results[0].get('municipio', 'N/A'))
        total = result.results[0].get('total_obitos', result.results[0].get('total_mortes', result.results[0].get('total', 'N/A')))
        print("\n" + "="*70)
        print(f"🏆 RESPOSTA DO AGENTE: {municipio} com {total} óbitos masculinos")
        print(f"✅ RESPOSTA ESPERADA: Ijuí com 212 óbitos masculinos")
        print(f"🎯 CORRETO: {'SIM' if str(municipio).upper() == 'IJUÍ' and int(total) == 212 else 'NÃO'}")
        print("="*70)
    else:
        print(f"❌ ERROR: {result.error_message}")
    
    # Test manual SQL for validation
    print(f"\n🔧 TESTE MANUAL COM SQL CORRETO:")
    correct_sql = """
    SELECT CIDADE_RESIDENCIA_PACIENTE, COUNT(*) as total_obitos 
    FROM sus_data 
    WHERE MORTE = 1 AND SEXO = 1 
    GROUP BY CIDADE_RESIDENCIA_PACIENTE 
    ORDER BY total_obitos DESC 
    LIMIT 1;
    """
    
    manual_result = orchestrator._query_service.execute_sql_query(correct_sql)
    if manual_result.success and manual_result.results:
        manual_municipio = manual_result.results[0]['CIDADE_RESIDENCIA_PACIENTE']
        manual_total = manual_result.results[0]['total_obitos']
        print(f"Manual result: {manual_municipio} com {manual_total} óbitos")
        
        if manual_municipio == 'Ijuí' and manual_total == 212:
            print("✅ VALIDAÇÃO MANUAL: CORRETO - Ijuí com 212 óbitos")
        else:
            print(f"⚠️ VALIDAÇÃO MANUAL: {manual_municipio} com {manual_total} óbitos")

if __name__ == "__main__":
    test_male_deaths_query()