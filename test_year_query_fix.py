#!/usr/bin/env python3
"""
Test script to validate the year query fix
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_year_query():
    """Test the year query with corrected SQL"""
    
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
    
    print("🔍 Testing query: 'Em qual ano teve o maior numero de mortes?'")
    print("=" * 60)
    
    # Process the problematic query
    result = orchestrator.process_single_query("Em qual ano teve o maior numero de mortes?")
    
    print(f"Success: {result.success}")
    print(f"SQL Generated: {result.sql_query}")
    print(f"Error: {result.error_message}")
    print(f"Results: {result.results}")
    print(f"Execution time: {result.execution_time:.2f}s")
    
    # If the original fails, test with corrected SQL manually
    if not result.success:
        print("\n🔧 Testing manual correction...")
        corrected_sql = "SELECT DT_INTER/10000 as ano, COUNT(*) as total_mortes FROM sus_data WHERE MORTE = 1 GROUP BY ano ORDER BY total_mortes DESC LIMIT 1;"
        
        manual_result = orchestrator._query_service.execute_sql_query(corrected_sql)
        print(f"Manual result success: {manual_result.success}")
        print(f"Manual results: {manual_result.results}")
        
        if manual_result.success and manual_result.results:
            ano = int(manual_result.results[0]['ano'])
            total_mortes = manual_result.results[0]['total_mortes']
            print(f"✅ RESPOSTA CORRETA: O ano com maior número de mortes foi {ano} com {total_mortes} mortes.")

if __name__ == "__main__":
    test_year_query()