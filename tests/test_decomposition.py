#!/usr/bin/env python3
"""
Test script to check decomposition system with debug logging
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.application.container.dependency_injection import ContainerFactory, ServiceConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator, OrchestratorConfig
from src.application.services.user_interface_service import InterfaceType

def test_decomposition():
    """Test the decomposition system with a complex query"""
    
    # Configuration with decomposition enabled and debug mode
    service_config = ServiceConfig(
        database_type="sqlite",
        database_path="../sus_database.db",
        llm_provider="ollama",
        llm_model="llama3",
        llm_temperature=0.0,
        llm_timeout=120,
        llm_max_retries=3,
        schema_type="sus",
        ui_type="cli",
        interface_type=InterfaceType.CLI_BASIC,
        error_handling_type="comprehensive",
        enable_error_logging=True,
        query_processing_type="comprehensive"
    )
    
    # Enable decomposition with debug mode
    orchestrator_config = OrchestratorConfig(
        max_query_length=2000,
        enable_query_history=True,
        enable_statistics=True,
        session_timeout=3600,
        enable_conversational_response=True,
        conversational_fallback=True,
        enable_query_routing=True,
        routing_confidence_threshold=0.7,
        # Decomposition settings
        enable_query_decomposition=True,
        decomposition_complexity_threshold=45.0,
        decomposition_timeout_seconds=120.0,
        decomposition_fallback_enabled=True,
        show_decomposition_progress=True,
        decomposition_debug_mode=True
    )
    
    # Create container and orchestrator
    container = ContainerFactory.create_container_with_config(service_config)
    orchestrator = Text2SQLOrchestrator(container, orchestrator_config)
    
    # Test complex query
    complex_query = """
    Análise detalhada das tendências de mortalidade por doenças respiratórias em mulheres acima de 60 anos nas principais cidades do Sul, comparando custos médios por procedimento com ranking de hospitais por especialização em pneumologia durante o último trimestre, incluindo correlação geográfica por latitude e longitude
    """
    
    print("🧪 Testing Decomposition System")
    print("=" * 60)
    print(f"Query: {complex_query.strip()}")
    print("=" * 60)
    
    # Enable decomposition debug mode
    orchestrator.set_decomposition_debug_mode(True)
    
    try:
        # Process the query
        result = orchestrator.process_single_query(complex_query.strip())
        
        print("\n" + "=" * 60)
        print("📊 RESULTS")
        print("=" * 60)
        
        print(f"Success: {result.success}")
        print(f"Execution Time: {result.execution_time:.2f}s")
        print(f"Row Count: {result.row_count}")
        
        if result.metadata:
            print(f"\n📋 Metadata:")
            for key, value in result.metadata.items():
                print(f"  {key}: {value}")
        
        if result.success:
            print(f"\n📄 SQL Query: {result.sql_query}")
            if result.results:
                print(f"📝 Results (first 3):")
                for i, row in enumerate(result.results[:3]):
                    print(f"  {i+1}. {row}")
        else:
            print(f"\n❌ Error: {result.error_message}")
        
        # Get decomposition statistics
        decomp_stats = orchestrator.get_decomposition_statistics()
        print(f"\n📈 Decomposition Statistics:")
        for key, value in decomp_stats.items():
            print(f"  {key}: {value}")
        
        # Get performance statistics
        try:
            perf_stats = orchestrator.get_performance_statistics()
            print(f"\n⚡ Performance Statistics:")
            for key, value in perf_stats.items():
                print(f"  {key}: {value}")
        except Exception as e:
            print(f"\n⚠️ Performance stats not available: {e}")
    
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        try:
            orchestrator.container.shutdown()
        except:
            pass

if __name__ == "__main__":
    test_decomposition()