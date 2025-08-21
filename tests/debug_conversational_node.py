#!/usr/bin/env python3
"""
Debug Conversational Node Issue
"""

import sys
import os

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def debug_conversational_node():
    """Debug the conversational node issue"""
    print("🔍 DEBUGGING CONVERSATIONAL NODE ISSUE")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.state import create_initial_state
        from src.langgraph_migration.nodes_refactored.conversational_node_pure import conversational_response_node_pure
        
        print("✅ Imports successful")
        
        # Create test state
        state = create_initial_state(
            user_query="O que significa CID J90?",
            session_id="debug_test"
        )
        
        # Simulate classification (conversational route)
        state["processing_route"] = "conversational"
        state["classification"] = {
            "query_type": "conversational_query",
            "confidence": 0.9
        }
        
        print("✅ State created")
        print(f"📋 State keys: {list(state.keys())}")
        
        # Call the node directly
        print("🔄 Calling conversational node...")
        result_state = conversational_response_node_pure(state)
        
        print("✅ Node execution completed")
        print(f"📊 Final response type: {type(result_state.get('final_response'))}")
        print(f"📝 Final response value: {repr(result_state.get('final_response'))}")
        
        conversational_response = result_state.get('conversational_response', {})
        print(f"💬 Conversational response: {type(conversational_response)}")
        print(f"💬 Conversational message: {repr(conversational_response.get('message', 'NO_MESSAGE'))}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_conversational_node()