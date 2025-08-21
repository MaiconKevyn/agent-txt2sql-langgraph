
import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.langgraph_migration.orchestrator_v3 import create_orchestrator
from src.langgraph_migration.tools.enhanced_list_tables_tool import EnhancedListTablesTool
from langchain_community.utilities import SQLDatabase

class TestToolsImplementation(unittest.TestCase):

    def setUp(self):
        """Set up a test database and orchestrator."""
        db_path = "test_tools.db"
        self.db_file = db_path
        
        # Create a simple in-memory database for testing
        self.db = SQLDatabase.from_uri(f"sqlite:///{db_path}")
        
        # Create a dummy table for the test
        self.db.run("CREATE TABLE dummy_table (id INTEGER, name TEXT)")
        self.db.run("INSERT INTO dummy_table (id, name) VALUES (1, 'test')")

        # Mock the orchestrator to avoid full initialization
        self.orchestrator = create_orchestrator(database_path=f"sqlite:///{db_path}")
        
        # Get the tools from the workflow
        self.tools = self.orchestrator._workflow.get_tools()
        self.list_tables_tool = None
        for tool in self.tools:
            if tool.name == "sql_db_list_tables":
                self.list_tables_tool = tool
                break

    def tearDown(self):
        """Remove the test database file."""
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

    def test_list_tables_tool_exists(self):
        """Test that the sql_db_list_tables tool is available in the agent."""
        self.assertIsNotNone(self.list_tables_tool, "sql_db_list_tables tool not found in agent's tools.")
        self.assertIsInstance(self.list_tables_tool, EnhancedListTablesTool, "Tool is not an instance of EnhancedListTablesTool.")

    def test_list_tables_tool_execution(self):
        """Test the execution of the enhanced list tables tool."""
        self.assertTrue(hasattr(self.list_tables_tool, '_run'), "Tool does not have a _run method.")
        
        # Execute the tool
        tool_output = self.list_tables_tool._run()

        # Assertions
        self.assertIsInstance(tool_output, str)
        self.assertIn("dummy_table", tool_output)
        self.assertIn("TABELAS COM DESCRIÇÕES", tool_output)
        self.assertIn("Tabela de dados dummy_table", tool_output) # Part of the default description

if __name__ == '__main__':
    unittest.main()
