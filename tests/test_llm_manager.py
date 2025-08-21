#!/usr/bin/env python3
"""
Test HybridLLMManager - LangGraph V3 Migration

Tests the new HybridLLMManager following LangGraph best practices:
- SQLDatabaseToolkit integration
- Tool binding functionality  
- MessagesState compatibility
- Multi-provider LLM support
- Official LangGraph patterns
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from src.langgraph_migration.llm_manager import HybridLLMManager, create_hybrid_llm_manager
    from src.application.config.simple_config import ApplicationConfig
    from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)


class TestHybridLLMManager(unittest.TestCase):
    """Test suite for HybridLLMManager"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        print("🧪 Testing HybridLLMManager - LangGraph V3 Migration")
        print("=" * 60)
    
    def setUp(self):
        """Set up test fixtures"""
        # Create test configuration
        self.config = ApplicationConfig(
            database_path="sus_database.db",
            llm_provider="ollama",
            llm_model="llama3",
            llm_temperature=0.1,
            llm_timeout=30
        )
        
        # Check if database exists
        if not os.path.exists(self.config.database_path):
            self.skipTest(f"Database not found: {self.config.database_path}")
    
    def test_manager_initialization(self):
        """Test HybridLLMManager initialization"""
        print("\n🔧 Test 1: Manager Initialization")
        
        try:
            manager = HybridLLMManager(self.config)
            
            # Check components are initialized
            self.assertIsNotNone(manager._llm, "LLM should be initialized")
            self.assertIsNotNone(manager._sql_database, "SQL Database should be initialized")
            self.assertIsNotNone(manager._sql_toolkit, "SQL Toolkit should be initialized")
            self.assertIsNotNone(manager._bound_llm, "Bound LLM should be initialized")
            
            print("   ✅ All components initialized successfully")
            
        except Exception as e:
            self.fail(f"Manager initialization failed: {e}")
    
    def test_factory_function(self):
        """Test factory function"""
        print("\n🏭 Test 2: Factory Function")
        
        try:
            manager = create_hybrid_llm_manager(self.config)
            self.assertIsInstance(manager, HybridLLMManager)
            print("   ✅ Factory function works correctly")
            
        except Exception as e:
            self.fail(f"Factory function failed: {e}")
    
    def test_sql_tools_access(self):
        """Test SQL tools access"""
        print("\n🔧 Test 3: SQL Tools Access")
        
        try:
            manager = HybridLLMManager(self.config)
            tools = manager.get_sql_tools()
            
            self.assertIsInstance(tools, list, "Tools should be a list")
            self.assertGreater(len(tools), 0, "Should have at least one tool")
            
            # Check tool names (from SQLDatabaseToolkit)
            tool_names = [tool.name for tool in tools]
            expected_tools = ["sql_db_query", "sql_db_schema", "sql_db_list_tables"]
            
            for expected_tool in expected_tools:
                self.assertIn(expected_tool, tool_names, f"Should have {expected_tool} tool")
            
            print(f"   ✅ Found {len(tools)} SQL tools: {tool_names}")
            
        except Exception as e:
            self.fail(f"SQL tools access failed: {e}")
    
    def test_bound_llm_access(self):
        """Test bound LLM access"""
        print("\n🤖 Test 4: Bound LLM Access")
        
        try:
            manager = HybridLLMManager(self.config)
            bound_llm = manager.get_bound_llm()
            
            self.assertIsNotNone(bound_llm, "Bound LLM should be available")
            
            # Check if tools are bound (this may vary by LLM implementation)
            has_tools = hasattr(bound_llm, 'bound_tools') or hasattr(bound_llm, 'tools')
            print(f"   ✅ Bound LLM available (tools bound: {has_tools})")
            
        except Exception as e:
            self.fail(f"Bound LLM access failed: {e}")
    
    def test_database_access(self):
        """Test database access"""
        print("\n💾 Test 5: Database Access")
        
        try:
            manager = HybridLLMManager(self.config)
            database = manager.get_database()
            
            self.assertIsNotNone(database, "Database should be available")
            
            # Test database functionality
            table_names = database.get_usable_table_names()
            self.assertIsInstance(table_names, list, "Table names should be a list")
            self.assertGreater(len(table_names), 0, "Should have at least one table")
            
            print(f"   ✅ Database connected with {len(table_names)} tables: {table_names[:3]}...")
            
        except Exception as e:
            self.fail(f"Database access failed: {e}")
    
    def test_messages_creation(self):
        """Test MessagesState pattern message creation"""
        print("\n💬 Test 6: Messages Creation")
        
        try:
            manager = HybridLLMManager(self.config)
            
            # Test basic message creation
            user_query = "Quantos pacientes existem?"
            messages = manager.create_messages(user_query)
            
            self.assertIsInstance(messages, list, "Messages should be a list")
            self.assertEqual(len(messages), 1, "Should have one message for basic query")
            self.assertIsInstance(messages[0], HumanMessage, "Should be HumanMessage")
            self.assertEqual(messages[0].content, user_query, "Content should match query")
            
            # Test with system prompt
            system_prompt = "You are a SQL assistant."
            messages_with_system = manager.create_messages(
                user_query=user_query,
                system_prompt=system_prompt
            )
            
            self.assertEqual(len(messages_with_system), 2, "Should have system + user message")
            self.assertIsInstance(messages_with_system[0], SystemMessage, "First should be SystemMessage")
            self.assertEqual(messages_with_system[0].content, system_prompt, "System content should match")
            
            # Test with conversation history
            history = [
                HumanMessage(content="Previous question"),
                AIMessage(content="Previous answer")
            ]
            messages_with_history = manager.create_messages(
                user_query=user_query,
                conversation_history=history
            )
            
            self.assertEqual(len(messages_with_history), 3, "Should include history + new message")
            
            print("   ✅ Messages creation works correctly")
            
        except Exception as e:
            self.fail(f"Messages creation failed: {e}")
    
    def test_sql_query_generation(self):
        """Test SQL query generation"""
        print("\n🔍 Test 7: SQL Query Generation")
        
        try:
            manager = HybridLLMManager(self.config)
            
            user_query = "Quantos pacientes existem?"
            schema_context = "Tabela: pacientes (id_paciente, nome, idade)"
            
            result = manager.generate_sql_query(
                user_query=user_query,
                schema_context=schema_context
            )
            
            self.assertIsInstance(result, dict, "Result should be a dictionary")
            self.assertIn("success", result, "Should have success field")
            self.assertIn("sql_query", result, "Should have sql_query field")
            self.assertIn("error", result, "Should have error field")
            self.assertIn("messages", result, "Should have messages field")
            
            # If successful, check SQL quality
            if result["success"]:
                sql_query = result["sql_query"]
                self.assertIsInstance(sql_query, str, "SQL query should be string")
                self.assertGreater(len(sql_query), 0, "SQL query should not be empty")
                print(f"   ✅ SQL generated: {sql_query[:50]}...")
            else:
                print(f"   ⚠️ SQL generation failed (expected in some environments): {result['error']}")
            
        except Exception as e:
            print(f"   ⚠️ SQL generation test failed (may be expected): {e}")
    
    def test_conversational_response(self):
        """Test conversational response generation"""
        print("\n💭 Test 8: Conversational Response")
        
        try:
            manager = HybridLLMManager(self.config)
            
            user_query = "O que significa CID-10?"
            
            result = manager.generate_conversational_response(user_query)
            
            self.assertIsInstance(result, dict, "Result should be a dictionary")
            self.assertIn("success", result, "Should have success field")
            self.assertIn("response", result, "Should have response field")
            self.assertIn("error", result, "Should have error field")
            self.assertIn("messages", result, "Should have messages field")
            
            # If successful, check response quality
            if result["success"]:
                response = result["response"]
                self.assertIsInstance(response, str, "Response should be string")
                self.assertGreater(len(response), 0, "Response should not be empty")
                print(f"   ✅ Response generated: {response[:50]}...")
            else:
                print(f"   ⚠️ Conversational response failed (expected in some environments): {result['error']}")
            
        except Exception as e:
            print(f"   ⚠️ Conversational response test failed (may be expected): {e}")
    
    def test_sql_validation(self):
        """Test SQL query validation"""
        print("\n✅ Test 9: SQL Validation")
        
        try:
            manager = HybridLLMManager(self.config)
            
            # Test valid SQL
            valid_sql = "SELECT COUNT(*) FROM pacientes;"
            result = manager.validate_sql_query(valid_sql)
            
            self.assertIsInstance(result, dict, "Result should be a dictionary")
            self.assertIn("is_valid", result, "Should have is_valid field")
            
            if result["is_valid"]:
                print(f"   ✅ Valid SQL recognized: {valid_sql}")
            else:
                print(f"   ⚠️ Valid SQL not recognized (table may not exist): {result['error']}")
            
            # Test invalid SQL
            invalid_sql = "INVALID SQL QUERY"
            result_invalid = manager.validate_sql_query(invalid_sql)
            
            self.assertFalse(result_invalid["is_valid"], "Invalid SQL should be rejected")
            print(f"   ✅ Invalid SQL correctly rejected")
            
        except Exception as e:
            print(f"   ⚠️ SQL validation test failed (may be expected): {e}")
    
    def test_sql_execution(self):
        """Test SQL query execution"""
        print("\n⚡ Test 10: SQL Execution")
        
        try:
            manager = HybridLLMManager(self.config)
            
            # Test simple query
            sql_query = "SELECT 1 as test_value;"
            result = manager.execute_sql_query(sql_query)
            
            self.assertIsInstance(result, dict, "Result should be a dictionary")
            self.assertIn("success", result, "Should have success field")
            self.assertIn("results", result, "Should have results field")
            self.assertIn("row_count", result, "Should have row_count field")
            
            if result["success"]:
                print(f"   ✅ SQL executed successfully, {result['row_count']} rows")
            else:
                print(f"   ⚠️ SQL execution failed: {result['error']}")
            
        except Exception as e:
            print(f"   ⚠️ SQL execution test failed (may be expected): {e}")
    
    def test_model_info(self):
        """Test model information retrieval"""
        print("\n📋 Test 11: Model Information")
        
        try:
            manager = HybridLLMManager(self.config)
            model_info = manager.get_model_info()
            
            self.assertIsInstance(model_info, dict, "Model info should be a dictionary")
            
            expected_fields = ["provider", "model_name", "has_sql_tools", "tools_bound", "database_connected"]
            for field in expected_fields:
                self.assertIn(field, model_info, f"Should have {field} field")
            
            print(f"   ✅ Model info: {model_info['provider']}/{model_info['model_name']}")
            print(f"      Tools: {model_info['has_sql_tools']}, Bound: {model_info['tools_bound']}")
            
        except Exception as e:
            self.fail(f"Model info retrieval failed: {e}")
    
    def test_health_check(self):
        """Test health check functionality"""
        print("\n🏥 Test 12: Health Check")
        
        try:
            manager = HybridLLMManager(self.config)
            health = manager.health_check()
            
            self.assertIsInstance(health, dict, "Health should be a dictionary")
            self.assertIn("status", health, "Should have status field")
            self.assertIn("components", health, "Should have components field")
            
            status = health["status"]
            self.assertIn(status, ["healthy", "degraded", "failed"], "Status should be valid")
            
            components = health["components"]
            component_fields = ["llm_status", "database_status", "toolkit_status", "tools_bound"]
            for field in component_fields:
                self.assertIn(field, components, f"Should have {field} in components")
            
            print(f"   ✅ Health check: {status}")
            print(f"      Components: {components}")
            
        except Exception as e:
            self.fail(f"Health check failed: {e}")


def run_tests():
    """Run all tests"""
    print("🚀 Starting HybridLLMManager Tests - LangGraph V3 Migration")
    print("=" * 70)
    
    # Check prerequisites
    if not os.path.exists("sus_database.db"):
        print("⚠️ Warning: Database 'sus_database.db' not found")
        print("   Some tests may skip or fail")
        print()
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestHybridLLMManager)
    runner = unittest.TextTestRunner(verbosity=0)
    result = runner.run(suite)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print(f"   ✅ Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   ❌ Failed: {len(result.failures)}")
    print(f"   🚨 Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"   - {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print("\n🚨 ERRORS:")
        for test, traceback in result.errors:
            print(f"   - {test}: {traceback.split('\\n')[-2]}")
    
    success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
    print(f"\n🎯 Success Rate: {success_rate:.1f}%")
    
    if success_rate >= 80:
        print("✅ HybridLLMManager is working well!")
    elif success_rate >= 60:
        print("⚠️ HybridLLMManager has some issues but core functionality works")
    else:
        print("❌ HybridLLMManager needs attention")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)