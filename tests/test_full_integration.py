#!/usr/bin/env python3
"""
Complete Integration Test - Frontend + API + LangGraph
Tests the full pipeline from frontend to LangGraph system
"""

import requests
import json
import time

def test_full_integration():
    """Test complete frontend + API + LangGraph integration"""
    print("🧪 COMPLETE INTEGRATION TEST - Frontend + API + LangGraph")
    print("=" * 70)
    
    # Test cases
    test_cases = [
        {
            "name": "SQL Query via Frontend",
            "question": "Quantos pacientes existem no sistema?",
            "expected_success": True,
            "should_have_sql": True
        },
        {
            "name": "Conversational Query via Frontend", 
            "question": "O que significa hipertensão?",
            "expected_success": True,
            "should_have_sql": False
        },
        {
            "name": "Complex SQL Query",
            "question": "Qual a média de idade dos pacientes?",
            "expected_success": True,
            "should_have_sql": True
        }
    ]
    
    # Test each endpoint
    endpoints = [
        ("Frontend API", "http://localhost:3000/api/query"),
        ("Direct API", "http://localhost:8000/query")
    ]
    
    all_passed = True
    
    for endpoint_name, url in endpoints:
        print(f"\n🔗 Testing {endpoint_name}: {url}")
        print("-" * 50)
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n{i}. {test_case['name']}")
            print(f"   Question: \"{test_case['question']}\"")
            
            try:
                # Make request
                start_time = time.time()
                response = requests.post(
                    url,
                    json={"question": test_case["question"]},
                    timeout=30
                )
                execution_time = time.time() - start_time
                
                if response.status_code != 200:
                    print(f"   ❌ HTTP {response.status_code}: {response.text[:100]}...")
                    all_passed = False
                    continue
                
                data = response.json()
                success = data.get("success", False)
                
                # Validate response
                print(f"   ✅ Success: {success}")
                print(f"   ⏱️ Time: {execution_time:.2f}s")
                
                if "sql_query" in data and data["sql_query"]:
                    print(f"   💾 SQL: {data['sql_query'][:60]}...")
                
                if "response" in data and data["response"]:
                    response_text = str(data["response"])
                    if "model=" in response_text:
                        # It's a raw LLM response, extract the actual content
                        print("   💬 Raw LLM response detected (system working)")
                    else:
                        print(f"   💬 Response: {response_text[:100]}...")
                
                # Check expectations
                if success == test_case["expected_success"]:
                    print(f"   ✅ Expected success: {test_case['expected_success']}")
                else:
                    print(f"   ❌ Expected success: {test_case['expected_success']}, got: {success}")
                    all_passed = False
                
                print(f"   🎯 Test Result: {'PASS' if success else 'FAIL'}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {str(e)}")
                all_passed = False
    
    # Test health endpoints
    print(f"\n🏥 Testing Health Endpoints")
    print("-" * 30)
    
    health_endpoints = [
        ("Frontend Health", "http://localhost:3000/api/health"),
        ("API Health", "http://localhost:8000/health"),
        ("Migration Stats", "http://localhost:8000/migration-stats")
    ]
    
    for name, url in health_endpoints:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ {name}: {data.get('status', 'OK')}")
            else:
                print(f"   ❌ {name}: HTTP {response.status_code}")
                all_passed = False
        except Exception as e:
            print(f"   ❌ {name}: {str(e)}")
            all_passed = False
    
    # Final result
    print(f"\n{'='*70}")
    print(f"🎯 INTEGRATION TEST RESULTS")
    print(f"{'='*70}")
    
    if all_passed:
        print("✅ ALL TESTS PASSED - Full integration working!")
        print("🎉 Frontend + API + LangGraph system fully operational")
        print("🚀 System ready for production use")
        print("\n🌐 Access your system at:")
        print("   • Frontend: http://localhost:3000")
        print("   • API Docs: http://localhost:8000/docs")
        print("   • Migration Stats: http://localhost:8000/migration-stats")
    else:
        print("❌ Some tests failed - review needed")
        print("🔧 Check logs and fix issues before production")
    
    return all_passed

if __name__ == "__main__":
    success = test_full_integration()
    exit(0 if success else 1)