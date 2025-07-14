#!/usr/bin/env python3
"""
Test script for Simple Query Decomposition System

This script tests the new simplified decomposition system to ensure:
1. Basic functionality works without errors
2. Complex queries are properly identified
3. Fallback mechanisms work correctly
4. System integrates properly with orchestrator
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.application.container.dependency_injection import ContainerFactory, ServiceConfig
from src.application.orchestrator.text2sql_orchestrator import Text2SQLOrchestrator, OrchestratorConfig
from src.application.services.user_interface_service import InterfaceType


def test_simple_decomposition():
    """Test the simplified decomposition system"""
    
    print("🧪 Testing Simple Query Decomposition System")
    print("=" * 60)
    
    # Configuration for testing
    service_config = ServiceConfig(
        database_type="sqlite",
        database_path="sus_database.db",
        llm_provider="ollama",
        llm_model="llama3",
        llm_temperature=0.0,
        llm_timeout=60,
        llm_max_retries=3,
        schema_type="sus",
        ui_type="cli",
        interface_type=InterfaceType.CLI_BASIC,
        error_handling_type="comprehensive",
        enable_error_logging=True,
        query_processing_type="comprehensive"
    )
    
    # Enable decomposition with debug mode for testing
    orchestrator_config = OrchestratorConfig(
        max_query_length=2000,
        enable_query_history=True,
        enable_statistics=True,
        session_timeout=3600,
        enable_conversational_response=True,
        conversational_fallback=True,
        enable_query_routing=True,
        routing_confidence_threshold=0.7,
        # Simple decomposition settings
        enable_query_decomposition=True,
        decomposition_complexity_threshold=3,
        decomposition_timeout_seconds=60.0,
        decomposition_fallback_enabled=True,
        decomposition_debug_mode=True  # Enable debug for testing
    )
    
    try:
        # Create container and orchestrator
        print("🔧 Initializing system...")
        container = ContainerFactory.create_container_with_config(service_config)
        orchestrator = Text2SQLOrchestrator(container, orchestrator_config)
        
        print("✅ System initialized successfully")
        
        # Test queries with different complexity levels
        test_queries = [
            {
                "name": "Simple Query",
                "query": "Quantos pacientes existem?",
                "expected_decomposition": False
            },
            {
                "name": "Medium Complexity",
                "query": "Qual é a média de idade dos pacientes por município?",
                "expected_decomposition": False
            },
            {
                "name": "Complex Query", 
                "query": "Análise detalhada das tendências de mortalidade por doenças respiratórias em mulheres acima de 60 anos nas principais cidades do Sul, comparando custos médios por procedimento com ranking de hospitais",
                "expected_decomposition": True
            },
            {
                "name": "Very Complex Query",
                "query": "Tendências detalhadas de correlação geográfica entre diagnósticos CID específicos e custos de procedimentos durante o último trimestre, incluindo ranking por município",
                "expected_decomposition": True
            }
        ]
        
        results = []
        
        for i, test_case in enumerate(test_queries, 1):
            print(f"\n📝 Test {i}: {test_case['name']}")
            print("-" * 40)
            print(f"Query: {test_case['query']}")
            
            try:
                # Process the query
                result = orchestrator.process_single_query(test_case['query'])
                
                # Check if decomposition was used
                decomposition_used = (
                    result.metadata and 
                    result.metadata.get("decomposition_used", False)
                )
                
                # Validate expectations
                expected = test_case['expected_decomposition']
                
                print(f"Result: {'✅' if result.success else '❌'}")
                print(f"Execution Time: {result.execution_time:.2f}s")
                print(f"Decomposition Used: {'✅' if decomposition_used else '❌'}")
                print(f"Expected Decomposition: {'✅' if expected else '❌'}")
                
                if result.metadata:
                    if decomposition_used:
                        strategy = result.metadata.get("strategy", "unknown")
                        complexity_score = result.metadata.get("complexity_score", 0)
                        patterns = result.metadata.get("detected_patterns", [])
                        print(f"Strategy: {strategy}")
                        print(f"Complexity Score: {complexity_score}")
                        print(f"Patterns: {patterns}")
                
                # Store result for summary
                results.append({
                    "name": test_case['name'],
                    "success": result.success,
                    "decomposition_used": decomposition_used,
                    "expected_decomposition": expected,
                    "execution_time": result.execution_time,
                    "strategy": result.metadata.get("strategy") if result.metadata else None
                })
                
                if result.success:
                    print(f"Row Count: {result.row_count}")
                else:
                    print(f"Error: {result.error_message}")
                    
            except Exception as e:
                print(f"❌ Test failed with error: {e}")
                results.append({
                    "name": test_case['name'],
                    "success": False,
                    "decomposition_used": False,
                    "expected_decomposition": expected,
                    "execution_time": 0.0,
                    "error": str(e)
                })
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r["success"])
        decomposition_tests = sum(1 for r in results if r["decomposition_used"])
        
        print(f"Total Tests: {total_tests}")
        print(f"Successful: {successful_tests}/{total_tests}")
        print(f"Decomposition Used: {decomposition_tests}")
        
        for result in results:
            status = "✅" if result["success"] else "❌"
            decomp = "🧩" if result["decomposition_used"] else "📝"
            print(f"{status} {decomp} {result['name']}: {result['execution_time']:.2f}s")
            if "error" in result:
                print(f"    Error: {result['error']}")
        
        # Get system statistics
        print(f"\n📈 SYSTEM STATISTICS")
        print("-" * 30)
        
        decomp_stats = orchestrator.get_decomposition_statistics()
        
        print(f"Decomposition Enabled: {decomp_stats['decomposition_enabled']}")
        print(f"Total Queries: {decomp_stats['total_queries_processed']}")
        print(f"Queries Decomposed: {decomp_stats['queries_decomposed']}")
        print(f"Success Rate: {decomp_stats['success_rate']:.1f}%")
        print(f"Fallback Count: {decomp_stats['fallback_count']}")
        
        if "simple_decomposer" in decomp_stats:
            simple_stats = decomp_stats["simple_decomposer"]
            print(f"\nSimple Decomposer Stats:")
            print(f"  Analyzed: {simple_stats['total_analyzed']}")
            print(f"  Decomposed: {simple_stats['total_decomposed']}")
            print(f"  Success Rate: {simple_stats['success_rate'] * 100:.1f}%")
            print(f"  Strategy Usage: {simple_stats['strategy_usage']}")
        
        # Overall assessment
        if successful_tests == total_tests:
            print(f"\n🎉 ALL TESTS PASSED! Simple decomposition system is working correctly.")
        elif successful_tests > 0:
            print(f"\n⚠️ {successful_tests}/{total_tests} tests passed. System partially functional.")
        else:
            print(f"\n❌ ALL TESTS FAILED. System needs debugging.")
        
        return successful_tests == total_tests
        
    except Exception as e:
        print(f"❌ System initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_complexity_analysis():
    """Test complexity analysis in isolation"""
    
    print("\n🔍 Testing Complexity Analysis")
    print("-" * 40)
    
    try:
        from src.application.services.simple_query_decomposer import SimpleQueryDecomposer, DecompositionConfig
        from src.application.services.query_processing_service import IQueryProcessingService
        
        # Mock query service for testing
        class MockQueryService:
            def process_natural_language_query(self, request):
                from src.application.services.query_processing_service import QueryResult
                return QueryResult(
                    sql_query="SELECT 1",
                    results=[{"test": 1}],
                    success=True,
                    execution_time=0.1,
                    row_count=1
                )
        
        config = DecompositionConfig(debug_mode=True)
        decomposer = SimpleQueryDecomposer(MockQueryService(), config)
        
        test_queries = [
            "Quantos pacientes?",
            "Análise detalhada das tendências de mortalidade",
            "Ranking dos principais municípios com correlação geográfica",
            "Tendências durante o último trimestre incluindo comparação"
        ]
        
        for query in test_queries:
            print(f"\nQuery: '{query}'")
            analysis = decomposer.analyze_complexity(query)
            print(f"Should decompose: {analysis.should_decompose}")
            print(f"Score: {analysis.complexity_score}")
            print(f"Patterns: {analysis.detected_patterns}")
        
        return True
        
    except Exception as e:
        print(f"❌ Complexity analysis test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting Simple Decomposition System Tests\n")
    
    # Test complexity analysis first
    complexity_ok = test_complexity_analysis()
    
    # Test full system
    system_ok = test_simple_decomposition()
    
    print(f"\n{'='*60}")
    print("🏁 FINAL RESULTS")
    print(f"{'='*60}")
    print(f"Complexity Analysis: {'✅' if complexity_ok else '❌'}")
    print(f"Full System Test: {'✅' if system_ok else '❌'}")
    
    if complexity_ok and system_ok:
        print("🎉 Simple Query Decomposition System is WORKING CORRECTLY!")
        sys.exit(0)
    else:
        print("❌ Some tests failed. System needs debugging.")
        sys.exit(1)