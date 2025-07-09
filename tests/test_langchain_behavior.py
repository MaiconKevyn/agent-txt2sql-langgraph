#!/usr/bin/env python3
"""
Test the LangChain agent behavior specifically to see if it generates incorrect JOINs
"""

import sys
import os

# Add parent directory and src to path
parent_dir = os.path.dirname(__file__)
sys.path.append(parent_dir)
sys.path.append(os.path.join(parent_dir, 'src'))

from src.application.container.dependency_injection import ContainerFactory, ServiceConfig
from src.application.services.user_interface_service import InterfaceType
from src.application.services.query_processing_service import (
    ComprehensiveQueryProcessingService, 
    QueryRequest
)

def main():
    print("🧪 Testing LangChain Agent SQL Generation")
    print("=" * 50)
    
    try:
        # Create configuration
        service_config = ServiceConfig(
            database_type="sqlite",
            database_path="../sus_database.db",
            llm_provider="ollama",
            llm_model="llama3",
            llm_temperature=0.0,
            llm_timeout=120,
            llm_max_retries=3,
            schema_type="sus",
            ui_type="cli",
            interface_type=InterfaceType.CLI_BASIC,
            error_handling_type="comprehensive",
            enable_error_logging=True,
            query_processing_type="comprehensive"
        )
        
        # Create dependency container
        container = ContainerFactory.create_container_with_config(service_config)
        container.initialize()
        
        # Get services
        from src.application.services.llm_communication_service import ILLMCommunicationService
        from src.application.services.database_connection_service import IDatabaseConnectionService
        from src.application.services.schema_introspection_service import ISchemaIntrospectionService
        from src.application.services.error_handling_service import IErrorHandlingService
        
        llm_service = container.get_service(ILLMCommunicationService)
        db_service = container.get_service(IDatabaseConnectionService) 
        schema_service = container.get_service(ISchemaIntrospectionService)
        error_service = container.get_service(IErrorHandlingService)
        
        # Create query processing service with LangChain as primary
        query_service = ComprehensiveQueryProcessingService(
            llm_service=llm_service,
            db_service=db_service,
            schema_service=schema_service,
            error_service=error_service,
            use_langchain_primary=True  # Force LangChain as primary method
        )
        
        # Test queries that might trigger problematic JOINs
        test_queries = [
            "list top 5 cities with most female deaths",
            "quantos pacientes com doenças respiratórias por cidade",  
            "top 5 cidades com mais mortes",
            "which cities have the highest number of deaths"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n🔍 Test {i}: {query}")
            print("-" * 50)
            
            # Create query request
            request = QueryRequest(user_query=query)
            
            # Process with LangChain agent as primary
            try:
                result = query_service.process_natural_language_query(request)
                
                print(f"✅ Success: {result.success}")
                print(f"📝 Generated SQL: {result.sql_query}")
                print(f"📊 Row Count: {result.row_count}")
                print(f"⚙️ Method Used: {result.metadata.get('method', 'unknown')}")
                
                # Check if SQL contains problematic patterns
                sql_lower = result.sql_query.lower()
                if 'join' in sql_lower and 'cid_capitulos' in sql_lower:
                    print("⚠️  WARNING: Query contains JOIN with cid_capitulos table")
                    print(f"   Full SQL: {result.sql_query}")
                elif 'join' in sql_lower:
                    print("⚠️  WARNING: Query contains JOIN")
                    print(f"   Full SQL: {result.sql_query}")
                elif 'group by' in sql_lower:
                    print("✅ Query uses proper GROUP BY (good)")
                else:
                    print("❓ Query doesn't use GROUP BY or JOIN")
                    
            except Exception as e:
                print(f"❌ Query failed: {e}")
                import traceback
                traceback.print_exc()
            
            print("=" * 50)
        
        print(f"\n🎉 LangChain agent behavior test completed!")
        
    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()