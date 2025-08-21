#!/usr/bin/env python3
"""
Debug Conversational Response Issue
"""

import sys
import os

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def debug_conversational():
    """Debug the conversational responder issue"""
    print("🔍 DEBUGGING CONVERSATIONAL RESPONSE ISSUE")
    print("=" * 60)
    
    try:
        from src.langgraph_migration.core.conversational_responder import get_conversational_responder
        
        print("✅ Import successful")
        
        # Get responder
        responder = get_conversational_responder()
        print("✅ Responder created")
        
        # Test direct response
        test_query = "O que significa CID J90?"
        print(f"🔍 Testing query: '{test_query}'")
        
        # Call generate_response directly
        result = responder.generate_response(test_query)
        
        print(f"📊 Response type: {type(result)}")
        print(f"📋 Response keys: {list(result.keys()) if isinstance(result, dict) else 'N/A'}")
        print(f"💬 Message type: {type(result.get('message')) if isinstance(result, dict) else 'N/A'}")
        print(f"💬 Message value: {repr(result.get('message')) if isinstance(result, dict) else 'N/A'}")
        
        if isinstance(result, dict) and result.get('message'):
            message = result['message']
            print(f"📝 Message preview: {message[:100]}...")
            print(f"✅ Message is valid: {bool(message and message.strip())}")
        else:
            print("❌ No valid message found")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_conversational()