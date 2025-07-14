#!/usr/bin/env python3
"""
Final test to validate the year query fix with complete answer
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.config.simple_config import ApplicationConfig, OrchestratorConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator

def test_year_query_complete():
    """Test the year query and display complete answer"""
    
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
    
    print("🔍 TESTE FINAL: 'Em qual ano teve o maior numero de mortes?'")
    print("=" * 70)
    
    # Process the query
    result = orchestrator.process_single_query("Em qual ano teve o maior numero de mortes?")
    
    print(f"✅ SUCCESS: {result.success}")
    print(f"🔧 SQL: {result.sql_query}")
    print(f"📊 RESULTS: {result.results}")
    print(f"⏱️ TIME: {result.execution_time:.2f}s")
    
    if result.success and result.results:
        ano = result.results[0]['ano']
        total_mortes = result.results[0]['total_mortes']
        print("\n" + "="*70)
        print(f"🏆 RESPOSTA FINAL: O ano com maior número de mortes foi {int(ano)} com {total_mortes} mortes.")
        print("="*70)
        return True
    else:
        print(f"❌ ERROR: {result.error_message}")
        return False

if __name__ == "__main__":
    success = test_year_query_complete()
    sys.exit(0 if success else 1)