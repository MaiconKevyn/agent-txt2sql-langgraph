#!/usr/bin/env python3
"""
Test MessagesState V3 - LangGraph Official Patterns

Tests the new MessagesState implementation following LangGraph best practices:
- MessagesState as primary state pattern
- Proper message handling and history
- Tool calling integration
- State transitions and validation
"""

import sys
import os
import unittest
from datetime import datetime

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from src.langgraph_migration.state_v3 import (
        MessagesStateTXT2SQL,
        QueryRoute,
        ExecutionPhase,
        QueryClassification,
        ToolCallResult,
        SQLExecutionResult,
        create_initial_messages_state,
        add_system_message,
        add_ai_message,
        add_tool_message,
        update_phase,
        add_error,
        add_tool_call_result,
        should_retry,
        get_conversation_history,
        format_for_llm_input,
        extract_sql_from_messages,
        get_latest_ai_response,
        calculate_success_metrics,
        state_to_legacy_format,
        validate_messages_state
    )
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)


class TestMessagesStateV3(unittest.TestCase):
    """Test suite for MessagesState V3"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        print("🧪 Testing MessagesState V3 - LangGraph Official Patterns")
        print("=" * 70)
    
    def setUp(self):
        """Set up test fixtures"""
        self.user_query = "Quantos pacientes existem no sistema?"
        self.session_id = "test_session_001"
        self.state = create_initial_messages_state(
            user_query=self.user_query,
            session_id=self.session_id
        )
    
    def test_initial_state_creation(self):
        """Test initial MessagesState creation"""
        print("\n🚀 Test 1: Initial State Creation")
        
        # Check required fields
        self.assertEqual(self.state["user_query"], self.user_query)
        self.assertEqual(self.state["session_id"], self.session_id)
        self.assertIsInstance(self.state["timestamp"], datetime)
        
        # Check messages
        self.assertEqual(len(self.state["messages"]), 1)
        self.assertIsInstance(self.state["messages"][0], HumanMessage)
        self.assertEqual(self.state["messages"][0].content, self.user_query)
        
        # Check initial state
        self.assertEqual(self.state["current_phase"], ExecutionPhase.INITIALIZATION)
        self.assertEqual(len(self.state["completed_phases"]), 0)
        self.assertFalse(self.state["success"])
        self.assertFalse(self.state["completed"])
        
        print("   ✅ Initial state created correctly with MessagesState pattern")
    
    def test_message_handling(self):
        """Test message handling functions"""
        print("\n💬 Test 2: Message Handling")
        
        # Add system message
        system_content = "You are a SQL assistant."
        state = add_system_message(self.state, system_content)
        
        messages = state["messages"]
        self.assertEqual(len(messages), 2)
        self.assertIsInstance(messages[-1], SystemMessage)
        self.assertEqual(messages[-1].content, system_content)
        
        # Add AI message
        ai_content = "I'll help you with SQL queries."
        state = add_ai_message(state, ai_content)
        
        messages = state["messages"]
        self.assertEqual(len(messages), 3)
        self.assertIsInstance(messages[-1], AIMessage)
        self.assertEqual(messages[-1].content, ai_content)
        
        # Add tool message
        tool_content = "Tool execution result"
        tool_call_id = "call_123"
        tool_name = "sql_db_query"
        state = add_tool_message(state, tool_call_id, tool_content, tool_name)
        
        messages = state["messages"]
        self.assertEqual(len(messages), 4)
        self.assertIsInstance(messages[-1], ToolMessage)
        self.assertEqual(messages[-1].content, tool_content)
        self.assertEqual(messages[-1].tool_call_id, tool_call_id)
        self.assertEqual(messages[-1].name, tool_name)
        
        print("   ✅ All message types handled correctly")
    
    def test_phase_management(self):
        """Test phase management and timing"""
        print("\n📊 Test 3: Phase Management")
        
        # Update to next phase
        execution_time = 1.5
        state = update_phase(
            self.state, 
            ExecutionPhase.QUERY_CLASSIFICATION,
            execution_time
        )
        
        # Check phase update
        self.assertEqual(state["current_phase"], ExecutionPhase.QUERY_CLASSIFICATION)
        self.assertIn(ExecutionPhase.INITIALIZATION, state["completed_phases"])
        
        # Check timing
        self.assertIn("query_classification", state["phase_timings"])
        self.assertEqual(state["phase_timings"]["query_classification"], execution_time)
        self.assertEqual(state["execution_time_total"], execution_time)
        
        # Update to another phase
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, 2.0)
        
        self.assertEqual(len(state["completed_phases"]), 2)
        self.assertEqual(state["execution_time_total"], 3.5)
        
        print("   ✅ Phase management and timing work correctly")
    
    def test_error_handling(self):
        """Test error handling functionality"""
        print("\n❌ Test 4: Error Handling")
        
        error_message = "SQL syntax error"
        error_type = "sql_syntax_error"
        phase = ExecutionPhase.SQL_GENERATION
        
        state = add_error(self.state, error_message, error_type, phase)
        
        # Check error was added
        self.assertEqual(len(state["errors"]), 1)
        self.assertEqual(state["current_error"], error_message)
        
        error_entry = state["errors"][0]
        self.assertEqual(error_entry["message"], error_message)
        self.assertEqual(error_entry["type"], error_type)
        self.assertEqual(error_entry["phase"], phase.value)
        
        print("   ✅ Error handling works correctly")
    
    def test_tool_call_management(self):
        """Test tool call result management"""
        print("\n🔧 Test 5: Tool Call Management")
        
        # Create tool call result
        tool_result = ToolCallResult(
            tool_name="sql_db_query",
            tool_input={"query": "SELECT COUNT(*) FROM patients"},
            tool_output="[(42,)]",
            success=True,
            execution_time=0.5
        )
        
        state = add_tool_call_result(self.state, tool_result)
        
        # Check tool call was added
        self.assertEqual(len(state["tool_calls"]), 1)
        self.assertEqual(state["tool_calls"][0], tool_result)
        
        # Check tool message was added
        messages = state["messages"]
        tool_messages = [msg for msg in messages if isinstance(msg, ToolMessage)]
        self.assertEqual(len(tool_messages), 1)
        self.assertEqual(tool_messages[0].name, "sql_db_query")
        
        print("   ✅ Tool call management works correctly")
    
    def test_retry_logic(self):
        """Test retry decision logic"""
        print("\n🔄 Test 6: Retry Logic")
        
        # Test retryable error
        self.assertTrue(should_retry(self.state, "sql_syntax_error"))
        self.assertTrue(should_retry(self.state, "tool_execution_error"))
        self.assertTrue(should_retry(self.state, "llm_timeout"))
        
        # Test non-retryable error
        self.assertFalse(should_retry(self.state, "unknown_error"))
        self.assertFalse(should_retry(self.state, "user_error"))
        
        # Test max retries exceeded
        state_max_retries = self.state.copy()
        state_max_retries["retry_count"] = 5
        state_max_retries["max_retries"] = 3
        
        self.assertFalse(should_retry(state_max_retries, "sql_syntax_error"))
        
        print("   ✅ Retry logic works correctly")
    
    def test_conversation_history(self):
        """Test conversation history extraction"""
        print("\n📖 Test 7: Conversation History")
        
        # Add various message types
        state = add_system_message(self.state, "System prompt")
        state = add_ai_message(state, "AI response")
        state = add_tool_message(state, "call_1", "Tool result", "sql_db_query")
        
        # Test with system messages
        history_with_system = get_conversation_history(state, include_system=True)
        self.assertEqual(len(history_with_system), 4)
        
        # Test without system messages
        history_without_system = get_conversation_history(state, include_system=False)
        self.assertEqual(len(history_without_system), 3)
        
        # Check first message is HumanMessage
        self.assertIsInstance(history_without_system[0], HumanMessage)
        
        print("   ✅ Conversation history extraction works correctly")
    
    def test_llm_input_formatting(self):
        """Test LLM input formatting"""
        print("\n🤖 Test 8: LLM Input Formatting")
        
        # Add some conversation
        state = add_ai_message(self.state, "I understand your question.")
        
        # Format for LLM input
        system_prompt = "You are a helpful SQL assistant."
        formatted_messages = format_for_llm_input(state, system_prompt)
        
        # Check structure
        self.assertGreater(len(formatted_messages), 0)
        self.assertIsInstance(formatted_messages[0], SystemMessage)
        self.assertEqual(formatted_messages[0].content, system_prompt)
        
        # Check conversation included
        human_messages = [msg for msg in formatted_messages if isinstance(msg, HumanMessage)]
        self.assertGreater(len(human_messages), 0)
        
        print("   ✅ LLM input formatting works correctly")
    
    def test_sql_extraction(self):
        """Test SQL extraction from messages"""
        print("\n🔍 Test 9: SQL Extraction")
        
        # Add AI message with SQL
        sql_content = "Here's your query:\n```sql\nSELECT COUNT(*) FROM patients;\n```"
        state = add_ai_message(self.state, sql_content)
        
        # Extract SQL
        extracted_sql = extract_sql_from_messages(state)
        
        self.assertIsNotNone(extracted_sql)
        self.assertEqual(extracted_sql, "SELECT COUNT(*) FROM patients;")
        
        # Test with plain SQL
        plain_sql_content = "SELECT * FROM doctors WHERE specialty = 'cardiology';"
        state = add_ai_message(state, plain_sql_content)
        
        extracted_plain = extract_sql_from_messages(state)
        self.assertIn("SELECT", extracted_plain)
        
        print("   ✅ SQL extraction works correctly")
    
    def test_latest_response_extraction(self):
        """Test latest AI response extraction"""
        print("\n💭 Test 10: Latest Response Extraction")
        
        # Add multiple AI messages
        state = add_ai_message(self.state, "First response")
        state = add_ai_message(state, "Second response")
        state = add_ai_message(state, "Latest response")
        
        # Get latest response
        latest = get_latest_ai_response(state)
        
        self.assertEqual(latest, "Latest response")
        
        print("   ✅ Latest response extraction works correctly")
    
    def test_success_metrics(self):
        """Test success metrics calculation"""
        print("\n📈 Test 11: Success Metrics")
        
        # Add some completed phases
        state = update_phase(self.state, ExecutionPhase.QUERY_CLASSIFICATION)
        state = update_phase(state, ExecutionPhase.SQL_GENERATION)
        
        # Add tool calls
        tool_result = ToolCallResult(
            tool_name="sql_db_query",
            tool_input={},
            tool_output="result",
            success=True,
            execution_time=1.0
        )
        state = add_tool_call_result(state, tool_result)
        
        # Calculate metrics
        metrics = calculate_success_metrics(state)
        
        # Check metrics structure
        expected_fields = [
            "overall_success", "completion_rate", "phases_completed",
            "total_phases", "tool_success_rate", "total_tools_used",
            "retry_count", "error_count", "execution_time"
        ]
        
        for field in expected_fields:
            self.assertIn(field, metrics)
        
        # Check values
        self.assertEqual(metrics["phases_completed"], 2)
        self.assertEqual(metrics["total_tools_used"], 1)
        self.assertEqual(metrics["tool_success_rate"], 1.0)
        
        print("   ✅ Success metrics calculation works correctly")
    
    def test_legacy_format_conversion(self):
        """Test conversion to legacy format"""
        print("\n🔄 Test 12: Legacy Format Conversion")
        
        # Set up state with some data
        state = self.state.copy()
        state["success"] = True
        state["final_response"] = "There are 100 patients in the system."
        state["generated_sql"] = "SELECT COUNT(*) FROM patients;"
        
        # Convert to legacy format
        legacy_result = state_to_legacy_format(state)
        
        # Check required fields
        required_fields = [
            "success", "question", "sql_query", "results", "row_count",
            "execution_time", "error_message", "response", "timestamp", "metadata"
        ]
        
        for field in required_fields:
            self.assertIn(field, legacy_result)
        
        # Check metadata includes LangGraph info
        metadata = legacy_result["metadata"]
        self.assertTrue(metadata["langgraph_v3"])
        self.assertTrue(metadata["messages_state"])
        self.assertIn("workflow_metrics", metadata)
        
        print("   ✅ Legacy format conversion works correctly")
    
    def test_state_validation(self):
        """Test state validation"""
        print("\n✅ Test 13: State Validation")
        
        # Test valid state
        issues = validate_messages_state(self.state)
        self.assertEqual(len(issues), 0, f"Valid state should have no issues, found: {issues}")
        
        # Test invalid state - missing required field
        invalid_state = self.state.copy()
        invalid_state["user_query"] = ""
        
        issues = validate_messages_state(invalid_state)
        self.assertGreater(len(issues), 0)
        self.assertTrue(any("user_query" in issue for issue in issues))
        
        # Test invalid state - missing messages
        invalid_state2 = self.state.copy()
        invalid_state2["messages"] = []
        
        issues2 = validate_messages_state(invalid_state2)
        self.assertGreater(len(issues2), 0)
        self.assertTrue(any("messages" in issue for issue in issues2))
        
        print("   ✅ State validation works correctly")


def run_tests():
    """Run all tests"""
    print("🚀 Starting MessagesState V3 Tests - LangGraph Official Patterns")
    print("=" * 80)
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMessagesStateV3)
    runner = unittest.TextTestRunner(verbosity=0)
    result = runner.run(suite)
    
    # Summary
    print("\n" + "=" * 80)
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
    
    if success_rate >= 90:
        print("✅ MessagesState V3 is working excellently!")
    elif success_rate >= 75:
        print("⚠️ MessagesState V3 is working well with minor issues")
    else:
        print("❌ MessagesState V3 needs attention")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)