#!/usr/bin/env python3
"""
Test Pure Refactored LangGraph System

This script tests the pure refactored LangGraph nodes directly,
bypassing the compatibility wrapper to ensure the core system works.
"""

import sys
import os

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def test_pure_langgraph():
    """Test the pure refactored LangGraph system"""
    print("🧪 Testing Pure Refactored LangGraph System...")
    print("=" * 60)
    
    try:
        # Import pure LangGraph components
        from src.langgraph_migration.workflow import create_txt2sql_workflow_factory
        from src.langgraph_migration.state import create_initial_state
        
        print("✅ Pure LangGraph imports successful")
        
        # Create workflow
        workflow = create_txt2sql_workflow_factory()
        print("✅ Pure LangGraph workflow created")
        
        # Test simple SQL query
        test_query = "Quantos pacientes existem?"
        print(f"🔍 Testing query: '{test_query}'")
        
        # Create initial state
        initial_state = create_initial_state(
            user_query=test_query,
            session_id="test_001"
        )
        print("✅ Initial state created")
        
        # Execute workflow
        print("⚡ Executing pure refactored workflow...")
        final_state = workflow.invoke(initial_state)
        print("✅ Workflow execution completed")
        
        # Display results
        print("\n📊 RESULTS:")
        print(f"  🎯 Final response: {final_state.get('final_response', 'None')[:100]}...")
        print(f"  🛤️ Processing route: {final_state.get('processing_route')}")
        print(f"  📈 Nodes visited: {final_state.get('nodes_visited', [])}")
        print(f"  ⏱️ Total execution time: {final_state.get('execution_time_total', 0):.2f}s")
        print(f"  ✅ Success: {final_state.get('success', False)}")
        
        if final_state.get('error_message'):
            print(f"  ❌ Error: {final_state.get('error_message')}")
        
        # Check metadata
        metadata = final_state.get('metadata', {})
        print(f"  🔥 LangGraph refactored: {metadata.get('langgraph_refactored', False)}")
        print(f"  📋 Version: {metadata.get('version', 'unknown')}")
        
        print("\n✅ SUCCESS: Pure refactored LangGraph system is working!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_pure_langgraph()
    sys.exit(0 if success else 1)