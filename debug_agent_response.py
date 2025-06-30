#!/usr/bin/env python3
"""
Script para debugar a resposta do agent e identificar onde está o problema
"""

import sys
import os
import sqlite3
import logging

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from application.services.query_processing_service import ComprehensiveQueryProcessingService, QueryRequest
from application.services.llm_communication_service import OllamaLLMCommunicationService, LLMConfig
from application.services.database_connection_service import SQLiteDatabaseConnectionService  
from application.services.schema_introspection_service import SUSSchemaIntrospectionService
from application.services.error_handling_service import ComprehensiveErrorHandlingService

# Setup logging to see details
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_agent_responses():
    """Test agent responses for date queries"""
    
    # Initialize services
    llm_config = LLMConfig(model_name="llama3", temperature=0)
    llm_service = OllamaLLMCommunicationService(llm_config)
    db_service = SQLiteDatabaseConnectionService('sus_database.db')
    schema_service = SUSSchemaIntrospectionService(db_service)
    error_service = ComprehensiveErrorHandlingService()
    
    # Create query processing service
    query_service = ComprehensiveQueryProcessingService(
        llm_service=llm_service,
        db_service=db_service, 
        schema_service=schema_service,
        error_service=error_service
    )
    
    # Test queries
    test_queries = [
        "quantos casos ao total foram encontrados entre abril de 2017 e julho de 2017?",
        "quantos casos entre janeiro de 2017 e maio de 2017?"
    ]
    
    for query in test_queries:
        print(f"\n{'='*80}")
        print(f"TESTING QUERY: {query}")
        print(f"{'='*80}")
        
        # Process query
        request = QueryRequest(user_query=query)
        result = query_service.process_natural_language_query(request)
        
        print(f"SQL Query: {result.sql_query}")
        print(f"Results: {result.results}")
        print(f"Row Count: {result.row_count}")
        print(f"Success: {result.success}")
        
        if result.metadata and 'agent_response' in result.metadata:
            print(f"\nFULL AGENT RESPONSE:")
            print("-" * 50)
            print(result.metadata['agent_response'])
            print("-" * 50)
        
        # Verify with direct database query
        conn = sqlite3.connect('sus_database.db')
        cursor = conn.cursor()
        
        if "abril" in query and "julho" in query:
            cursor.execute("SELECT COUNT(*) FROM sus_data WHERE DT_INTER >= 20170401 AND DT_INTER <= 20170731")
            expected_count = cursor.fetchone()[0]
            print(f"\nDIRECT DB QUERY (April-July 2017): {expected_count}")
        elif "janeiro" in query and "maio" in query:
            cursor.execute("SELECT COUNT(*) FROM sus_data WHERE DT_INTER >= 20170101 AND DT_INTER <= 20170531")
            expected_count = cursor.fetchone()[0]
            print(f"\nDIRECT DB QUERY (Jan-May 2017): {expected_count}")
        
        conn.close()
        
        print(f"\nMATCH: {result.row_count == expected_count if 'expected_count' in locals() else 'Cannot verify'}")

if __name__ == "__main__":
    test_agent_responses()