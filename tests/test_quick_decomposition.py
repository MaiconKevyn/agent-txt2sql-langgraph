#!/usr/bin/env python3
"""
Quick test for Simple Query Decomposition System
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

def test_complexity_analysis():
    """Test just the complexity analysis part"""
    
    print("🔍 Testing Complexity Analysis Only")
    print("-" * 40)
    
    try:
        from src.application.services.simple_query_decomposer import SimpleQueryDecomposer, DecompositionConfig
        
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
        
        config = DecompositionConfig(debug_mode=True, complexity_threshold=2)
        decomposer = SimpleQueryDecomposer(MockQueryService(), config)
        
        test_queries = [
            ("Simple", "Quantos pacientes?", False),
            ("Medium", "Qual a média de idade por município?", False), 
            ("Complex", "Análise detalhada das tendências de mortalidade", True),
            ("Very Complex", "Ranking dos principais municípios com correlação geográfica durante último trimestre incluindo comparação", True)
        ]
        
        print("Results:")
        all_correct = True
        
        for name, query, expected in test_queries:
            print(f"\n{name}: '{query}'")
            analysis = decomposer.analyze_complexity(query)
            actual = analysis.should_decompose
            correct = actual == expected
            all_correct &= correct
            
            status = "✅" if correct else "❌"
            print(f"{status} Should decompose: {actual} (expected: {expected})")
            print(f"   Score: {analysis.complexity_score}")
            print(f"   Patterns: {analysis.detected_patterns}")
            print(f"   Strategy: {analysis.recommended_strategy}")
        
        print(f"\n{'='*40}")
        print(f"Overall: {'✅ ALL CORRECT' if all_correct else '❌ SOME FAILED'}")
        
        # Test statistics
        stats = decomposer.get_statistics()
        print(f"\nStatistics:")
        print(f"  Total analyzed: {stats['total_analyzed']}")
        print(f"  Configuration threshold: {stats['configuration']['complexity_threshold']}")
        
        return all_correct
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_system_integration():
    """Test basic system integration without LLM calls"""
    
    print("\n🔧 Testing System Integration")
    print("-" * 40)
    
    try:
        from src.application.orchestrator.text2sql_orchestrator import OrchestratorConfig
        
        # Test configuration
        config = OrchestratorConfig(
            enable_query_decomposition=True,
            decomposition_complexity_threshold=3,
            decomposition_debug_mode=True
        )
        
        print(f"✅ Configuration created:")
        print(f"   Decomposition enabled: {config.enable_query_decomposition}")
        print(f"   Threshold: {config.decomposition_complexity_threshold}")
        print(f"   Debug mode: {config.decomposition_debug_mode}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Quick Simple Decomposition System Test\n")
    
    # Test components
    complexity_ok = test_complexity_analysis()
    integration_ok = test_system_integration()
    
    print(f"\n{'='*50}")
    print("🏁 QUICK TEST RESULTS")
    print(f"{'='*50}")
    print(f"Complexity Analysis: {'✅' if complexity_ok else '❌'}")
    print(f"System Integration: {'✅' if integration_ok else '❌'}")
    
    if complexity_ok and integration_ok:
        print("\n🎉 Simple Query Decomposition System BASIC FUNCTIONALITY WORKING!")
        print("💡 System is ready for real queries (though fallback to standard processing)")
    else:
        print("\n❌ Some basic tests failed.")
        
    sys.exit(0 if (complexity_ok and integration_ok) else 1)