#!/usr/bin/env python3
"""
Demo script to show interactive routing
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.application.container.dependency_injection import (
    ContainerFactory, 
    ServiceConfig
)
from src.application.orchestrator.text2sql_orchestrator import (
    Text2SQLOrchestrator,
    OrchestratorConfig
)
from src.application.services.user_interface_service import InterfaceType


def demo_routing():
    """Demo the routing functionality with different query types"""
    print("🚀 Demo do Sistema de Roteamento de Queries")
    print("=" * 50)
    
    # Setup
    service_config = ServiceConfig(
        database_path="sus_database.db",
        enable_query_classification=True,
        interface_type=InterfaceType.CLI_INTERACTIVE,
        llm_model="llama3"
    )
    
    orchestrator_config = OrchestratorConfig(
        enable_query_routing=True,
        routing_confidence_threshold=0.7
    )
    
    container = ContainerFactory.create_container_with_config(service_config)
    orchestrator = Text2SQLOrchestrator(container, orchestrator_config)
    
    # Test queries
    test_queries = [
        "O que significa CID J90?",
        "Quantos pacientes existem?"
    ]
    
    for query in test_queries:
        print(f"\n▶️ Processando: {query}")
        print("-" * 40)
        
        # Process query and format response
        result = orchestrator.process_single_query(query)
        formatted_response = orchestrator._format_query_result(result, query)
        
        # Use interactive display
        ui_service = container.get_service(
            container._container.__class__.__bases__[0]  # Get interface
        )
        
        # Simulate interactive display
        if formatted_response.metadata and formatted_response.metadata.get("routing_applied"):
            query_type = formatted_response.metadata.get("query_classification", "unknown")
            confidence = formatted_response.metadata.get("classification_confidence", 0.0)
            
            if query_type == "conversational_query":
                print(f"💬 Pergunta conversacional identificada (confiança: {confidence:.2f})")
            elif query_type == "database_query":
                print(f"🔍 Consulta de banco de dados identificada (confiança: {confidence:.2f})")
            
            routing_method = formatted_response.metadata.get("routing_method")
            if routing_method:
                if routing_method == "direct_conversational":
                    print(f"🎯 Processamento: Resposta conversacional direta")
                elif routing_method in ["sql_processing", "direct_llm_fallback"]:
                    print(f"🎯 Processamento: Análise de banco de dados")
        
        print(f"\n✅ Resultado da consulta:")
        print(f"📊 {formatted_response.content}")
        
        if formatted_response.execution_time:
            print(f"⏱️ Tempo: {formatted_response.execution_time:.2f}s")


if __name__ == "__main__":
    demo_routing()