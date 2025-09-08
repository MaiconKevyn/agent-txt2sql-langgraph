import sys
import argparse
from typing import Optional
from dotenv import load_dotenv
import os

# Add project root to path
project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.append(project_root)

# Load environment variables for LangSmith tracing
load_dotenv()

# Import the clean architecture components
from src.application.config.simple_config import (
    ApplicationConfig,
    OrchestratorConfig,
    InterfaceType
)
# Import TXT2SQL Agent orchestrator
from src.agent.orchestrator import (
    LangGraphOrchestrator
)


def create_app_config(args) -> ApplicationConfig:
    """Create application configuration from command line arguments"""
    # Use defaults from ApplicationConfig and override with command line args when explicitly provided
    config = ApplicationConfig()
    
    # Override with command line arguments if they differ from defaults
    # if hasattr(args, 'database_path') and args.database_path != "sus_database.db":
    #     config.database_path = args.database_path
    if hasattr(args, 'model') and args.model != "llama3":
        # Only override if user explicitly specified a different model
        config.llm_model = args.model
    if hasattr(args, 'timeout') and args.timeout != 120:
        config.llm_timeout = args.timeout
    if hasattr(args, 'interactive'):
        config.interface_type = InterfaceType.CLI_INTERACTIVE if args.interactive else InterfaceType.CLI_BASIC
    if hasattr(args, 'enable_logging'):
        config.enable_error_logging = args.enable_logging
    
    return config


def create_orchestrator_config(args) -> OrchestratorConfig:
    """Create orchestrator configuration from command line arguments"""
    return OrchestratorConfig(
        max_query_length=1000,
        enable_query_history=True,
        enable_statistics=True,
        session_timeout=3600,
        enable_conversational_response=True,  # Enable multi-LLM conversational responses
        conversational_fallback=True          # Enable fallback if conversational LLM fails
    )


def debug_query_execution(orchestrator, user_query: str):
    """
    Execute query with detailed step-by-step debugging
    
    Args:
        orchestrator: LangGraphOrchestrator instance
        user_query: User's natural language question
    """
    import time
    
    print(" DEBUG MODE: Workflow Step-by-Step Analysis")
    print("=" * 70)
    print(f" Query: {user_query}")
    print("=" * 70)
    
    # Track debug data
    debug_data = {
        "question": user_query,
        "start_time": time.time(),
        "steps": [],
        "classification": None,
        "tables_discovered": None,
        "tables_selected": None,
        "sql_generated": None,
        "sql_validated": None,
        "execution_results": None,
        "final_response": None
    }
    
    step_counter = 1
    
    try:
        # Create config for streaming with checkpointer
        config = {
            "configurable": {
                "thread_id": f"debug_{hash(user_query) % 10000}"
            }
        }
        
        # Create proper initial state
        from src.agent.state import create_initial_messages_state
        
        initial_state = create_initial_messages_state(
            user_query=user_query,
            session_id=f"debug_{hash(user_query) % 10000}"
        )
        
        # Use process_query with streaming to maintain LangSmith integration
        session_id = f"debug_{hash(user_query) % 10000}"
        results = orchestrator.process_query(
            user_query=user_query,
            session_id=session_id,
            streaming=True,
            config=config,
            run_name=f"debug_query_{session_id}",
            tags=["debug", "txt2sql_agent_clean"],
            metadata={"debug_mode": True, "script": "txt2sql_agent_clean.py"}
        )
        
        # Process streaming results
        for update in results:
            for node_name, node_state in update.items():
                
                print(f"\n STEP {step_counter}: {node_name.upper()}")
                print("-" * 50)
                
                # Extract and display relevant data based on node type
                step_data = {"node": node_name, "data": {}}
                
                # 1. Classification Node
                if node_name == "query_classification":
                    route = node_state.get("query_route")
                    classification = node_state.get("classification", {})
                    confidence = classification.get("confidence_score", "N/A")
                    
                    debug_data["classification"] = str(route)
                    step_data["data"]["route"] = str(route)
                    step_data["data"]["confidence"] = confidence
                    
                    print(f" Classification: {route}")
                    print(f" Confidence: {confidence}")
                    if route:
                        print(f"   Next: {'SQL Pipeline' if str(route).endswith('DATABASE') else 'Direct Response'}")
                
                # 2. Table Discovery Node
                elif node_name == "list_tables":
                    available = node_state.get("available_tables", [])
                    selected = node_state.get("selected_tables", [])
                    
                    debug_data["tables_discovered"] = available
                    debug_data["tables_selected"] = selected
                    step_data["data"]["available_tables"] = available
                    step_data["data"]["selected_tables"] = selected
                    
                    print(f"  Tables Available: {len(available)}")
                    print(f"     Full list: {available[:5]}{'...' if len(available) > 5 else ''}")
                    print(f" Tables Selected: {len(selected)}")
                    print(f"     Selected: {selected}")
                    
                    if selected:
                        if "mortes" in selected:
                            print(f"     Great! Selected 'mortes' table for death queries")
                        if "procedimentos" in selected:
                            print(f"     Great! Selected 'procedimentos' table for procedure queries")
                
                # 3. Schema Node
                elif node_name == "get_schema":
                    schema_context = node_state.get("schema_context", "")
                    enhanced_mappings = node_state.get("enhanced_with_sus_mappings", False)
                    
                    step_data["data"]["schema_size"] = len(schema_context)
                    step_data["data"]["sus_enhanced"] = enhanced_mappings
                    
                    print(f" Schema Context: {len(schema_context)} characters")
                    print(f" SUS Mappings: {' Enhanced' if enhanced_mappings else ' Not enhanced'}")
                    
                    # Show partial schema for debug
                    if schema_context and len(schema_context) > 100:
                        print(f"     Schema preview: {schema_context[:100]}...")
                
                # 4. SQL Generation Node
                elif node_name == "generate_sql":
                    sql = node_state.get("generated_sql", "")
                    selected_tables = node_state.get("selected_tables", [])
                    
                    debug_data["sql_generated"] = sql
                    step_data["data"]["sql"] = sql
                    step_data["data"]["tables_used"] = selected_tables
                    
                    print(f" SQL Generated:")
                    print(f"     Query: {sql}")
                    print(f"      Using tables: {selected_tables}")
                    
                    # Validate SQL quality
                    if sql:
                        if "COUNT(*)" in sql.upper():
                            print(f"     Count query detected")
                        if any(table in sql.lower() for table in ["mortes", "procedimentos"]):
                            print(f"     Using specialized healthcare tables")
                        if "SELECT *" in sql.upper():
                            print(f"      Warning: SELECT * detected (might be inefficient)")
                
                # 5. SQL Validation Node
                elif node_name == "validate_sql":
                    validated_sql = node_state.get("validated_sql", "")
                    validation_errors = node_state.get("validation_errors", [])
                    
                    debug_data["sql_validated"] = validated_sql
                    step_data["data"]["validated_sql"] = validated_sql
                    step_data["data"]["errors"] = validation_errors
                    
                    print(f" SQL Validation:")
                    if validated_sql:
                        print(f"     Validation passed")
                        print(f"     Validated SQL: {validated_sql}")
                    
                    if validation_errors:
                        print(f"     Validation errors: {validation_errors}")
                
                # 6. SQL Execution Node
                elif node_name == "execute_sql":
                    execution_result = node_state.get("sql_execution_result")
                    if hasattr(execution_result, 'results'):
                        # SQLExecutionResult object
                        results = execution_result.results or []
                        success = execution_result.success
                        error = execution_result.error_message or ""
                    else:
                        # Dictionary format
                        results = execution_result.get("results", []) if execution_result else []
                        success = execution_result.get("success", False) if execution_result else False
                        error = execution_result.get("error_message", "") if execution_result else ""
                    
                    debug_data["execution_results"] = {
                        "success": success,
                        "row_count": len(results),
                        "first_row": results[0] if results else None
                    }
                    step_data["data"]["execution"] = debug_data["execution_results"]
                    
                    print(f" SQL Execution:")
                    if success:
                        print(f"     Execution successful")
                        print(f"     Results: {len(results)} rows returned")
                        if results:
                            print(f"     First row: {results[0]}")
                            # Handle different result formats
                            first_row = results[0]
                            if isinstance(first_row, dict) and 'result' in first_row:
                                # Extract count from string format
                                result_str = first_row['result']
                                if result_str.startswith('[') and result_str.endswith(']'):
                                    try:
                                        # Parse [(569405,)] format
                                        import ast
                                        parsed_result = ast.literal_eval(result_str)
                                        if parsed_result and len(parsed_result) > 0:
                                            count_value = parsed_result[0][0] if isinstance(parsed_result[0], tuple) else parsed_result[0]
                                            print(f"     Count result: {count_value:,}")
                                    except:
                                        pass
                            elif isinstance(first_row, (list, tuple)) and len(first_row) == 1:
                                count_value = first_row[0]
                                if isinstance(count_value, (int, float)):
                                    print(f"     Count result: {count_value:,}")
                    else:
                        print(f"     Execution failed: {error}")
                
                # 7. Response Generation Node
                elif node_name == "generate_response":
                    # Extract response from the correct field
                    response = node_state.get("final_response", "") or node_state.get("response", "")
                    success = node_state.get("success", False)
                    completed = node_state.get("completed", False)
                    
                    debug_data["final_response"] = response
                    step_data["data"]["response"] = response
                    step_data["data"]["success"] = success
                    step_data["data"]["completed"] = completed
                    
                    print(f" Response Generation:")
                    print(f"     Final response: {response}")
                    print(f"     Success: {success}")
                    print(f"     Completed: {completed}")
                
                # 8. Generic state info
                else:
                    # Show any other relevant state information
                    relevant_keys = [k for k in node_state.keys() if not k.startswith("_")]
                    if relevant_keys:
                        print(f" State keys: {relevant_keys}")
                
                # Add step to debug data
                debug_data["steps"].append(step_data)
                step_counter += 1
                
                print("-" * 50)
        
        # Final summary
        total_time = time.time() - debug_data["start_time"]
        
        print(f"\n DEBUG SUMMARY")
        print("=" * 70)
        print(f" Query: {debug_data['question']}")
        print(f" Classification: {debug_data['classification']}")
        print(f"  Tables discovered: {len(debug_data['tables_discovered']) if debug_data['tables_discovered'] else 0}")
        print(f" Tables selected: {debug_data['tables_selected']}")
        print(f" SQL generated: {debug_data['sql_generated']}")
        if debug_data['execution_results']:
            results = debug_data['execution_results']
            print(f" Execution: {' Success' if results['success'] else ' Failed'} ({results['row_count']} rows)")
        print(f" Response: {debug_data['final_response'][:100] if debug_data['final_response'] else 'N/A'}{'...' if debug_data['final_response'] and len(debug_data['final_response']) > 100 else ''}")
        print(f"   Total time: {total_time:.2f}s")
        print(f" Steps executed: {len(debug_data['steps'])}")
        
    except KeyboardInterrupt:
        print(f"\n\n Debug interrupted by user")
    except Exception as e:
        print(f"\n Debug error: {str(e)}")
        import traceback
        traceback.print_exc()


def start_interactive_debug_session(orchestrator):
    """
    Start interactive session with debug mode enabled
    """
    print(" TEXT2SQL DEBUG MODE - Interactive Session")
    print("=" * 60)
    print("Digite 'exit', 'quit' ou 'sair' para sair")
    print("Cada query será executada com debug detalhado")
    print("=" * 60)
    
    while True:
        try:
            user_input = input("\n Sua pergunta (debug mode): ").strip()
            
            # Handle exit commands
            if user_input.lower() in ['exit', 'quit', 'sair']:
                print("\n Até logo!")
                break
            elif not user_input:
                continue
            
            # Execute query with debug
            print()  # Empty line for better readability
            debug_query_execution(orchestrator, user_input)
            
        except KeyboardInterrupt:
            print("\n\n Até logo!")
            break
        except Exception as e:
            print(f"\n Erro interno: {str(e)}")
            print("Digite 'exit' para sair ou tente outra pergunta.")


def main():
    """Main entry point with clean architecture"""
    parser = argparse.ArgumentParser(
        description="TXT2SQL Claude - Arquitetura Limpa seguindo Princípios SOLID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python txt2sql_agent_clean.py                    # Usar configurações padrão
  python txt2sql_agent_clean.py --model mistral    # Usar modelo Mistral
  python txt2sql_agent_clean.py --basic            # Interface básica (sem emojis)
  python txt2sql_agent_clean.py --query "Quantas mortes em Porto Alegre?"  # Query única
  python txt2sql_agent_clean.py --query "Quantas mortes?" --debug-steps    # Query com debug detalhado
  python txt2sql_agent_clean.py --debug-steps      # Modo interativo com debug

Arquitetura:
  Este agente segue os princípios SOLID com separação clara de responsabilidades:
  • DatabaseConnectionService: Gerencia conexões com banco
  • LLMCommunicationService: Comunica com modelos LLM
  • SchemaIntrospectionService: Analisa schema do banco
  • UserInterfaceService: Gerencia interação com usuário
  • ErrorHandlingService: Trata todos os erros
  • QueryProcessingService: Processa consultas
  • DependencyContainer: Injeta dependências
  • Text2SQLOrchestrator: Coordena todos os serviços
        """
    )
    
    # Database options
    parser.add_argument(
        "--database-path", 
        default="sus_database.db",
        help="Caminho para o banco de dados SQLite (padrão: sus_database.db)"
    )
    
    # LLM options
    parser.add_argument(
        "--model", 
        default="llama3",
        help="Modelo LLM para usar (padrão: llama3)"
    )
    parser.add_argument(
        "--timeout", 
        type=int, 
        default=120,
        help="Timeout para requisições LLM em segundos (padrão: 120)"
    )
    
    # Interface options
    parser.add_argument(
        "--interactive", 
        action="store_true",
        help="Usar interface interativa com emojis (padrão)"
    )
    parser.add_argument(
        "--basic", 
        action="store_true",
        help="Usar interface básica sem emojis"
    )
    
    # Query options
    parser.add_argument(
        "--query", 
        type=str,
        help="Executar uma única consulta e sair"
    )
    
    # System options
    parser.add_argument(
        "--enable-logging", 
        action="store_true", 
        default=True,
        help="Habilitar logging de erros (padrão: habilitado)"
    )
    parser.add_argument(
        "--disable-logging", 
        action="store_true",
        help="Desabilitar logging de erros"
    )
    parser.add_argument(
        "--health-check", 
        action="store_true",
        help="Executar verificação de saúde do sistema e sair"
    )
    parser.add_argument(
        "--version", 
        action="store_true",
        help="Mostrar informações de versão e arquitetura"
    )
    parser.add_argument(
        "--visualize-workflow", 
        action="store_true",
        help="Gerar diagrama visual do workflow LangGraph e sair"
    )
    parser.add_argument(
        "--debug-workflow", 
        action="store_true",
        help="Mostrar estrutura textual do workflow LangGraph e sair"
    )
    parser.add_argument(
        "--debug-steps", 
        action="store_true",
        help="Mostrar estados detalhados de cada step do workflow durante execução"
    )
    
    args = parser.parse_args()
    
    # Handle version info
    if args.version:
        print("""
TXT2SQL Claude - LangGraph V3
====================================

Versão: 3.0.0
Arquitetura: LangGraph SQL Agent com PostgreSQL
Última atualização: Agosto 2025

Componentes LangGraph V3:
• LangGraphOrchestrator: Orquestração principal
• Query Classification Node: Roteamento inteligente
• Table Discovery Node: Seleção inteligente de tabelas
• Schema Introspection Node: Análise de schema
• SQL Generation Node: Geração de SQL
• SQL Validation Node: Validação de SQL
• SQL Execution Node: Execução no PostgreSQL
• Response Generation Node: Formatação de resposta

Melhorias da V3:
 Migração completa para LangGraph
 PostgreSQL com 15 tabelas especializadas
 Seleção inteligente de tabelas (75%+ precisão)
 Suporte a healthcare brasileiro (SUS)
 Visualização de workflow (--visualize-workflow)
 Debug interativo com retry mechanisms
 Multi-LLM support (Ollama, HuggingFace)

Database: PostgreSQL sih_rs (11M+ registros)
Domínio: Healthcare brasileiro (mortes, procedimentos, internações)
""")
        return
    
    # Process arguments
    if args.disable_logging:
        args.enable_logging = False
    
    if args.basic:
        args.interactive = False
    else:
        args.interactive = True
    
    try:
        # Create configuration
        app_config = create_app_config(args)
        orchestrator_config = create_orchestrator_config(args)
        
        # Create orchestrator directly with configurations (LangGraph V3)
        orchestrator = LangGraphOrchestrator(app_config, orchestrator_config)
        
        # Health check mode
        if args.health_check:
            print(" Executando verificação de saúde do sistema...")
            health_status = orchestrator.health_check()
            
            print(f"\n Status do Sistema: {health_status['status'].upper()}")
            print("=" * 50)
            
            for service_name, service_health in health_status['services'].items():
                status_icon = "" if service_health.get('healthy', False) else ""
                print(f"{status_icon} {service_name.title()}: {'OK' if service_health.get('healthy', False) else 'ERRO'}")
            
            if health_status['status'] != 'healthy':
                print(f"\n Sistema não está completamente saudável")
                sys.exit(1)
            else:
                print(f"\n Sistema funcionando perfeitamente!")
            return
        
        # Workflow visualization mode
        if args.visualize_workflow:
            print(" Gerando diagrama visual do workflow LangGraph...")
            try:
                orchestrator.save_workflow_diagram("langgraph_workflow.png", xray=True)
                print(" Diagrama visual salvo como 'langgraph_workflow.png'")
                print(" Dica: Abra o arquivo PNG para ver o fluxo completo do agente")
            except Exception as e:
                print(f" Erro ao gerar diagrama: {str(e)}")
                sys.exit(1)
            return
        
        # Workflow structure debug mode
        if args.debug_workflow:
            print(" Exibindo estrutura textual do workflow LangGraph...")
            try:
                orchestrator.print_workflow_structure()
                print("\n Use --visualize-workflow para gerar diagrama PNG")
            except Exception as e:
                print(f" Erro ao exibir estrutura: {str(e)}")
                sys.exit(1)
            return
        
        # Orchestrator already created above for health check compatibility
        
        # Single query mode
        if args.query:
            if args.debug_steps:
                # Debug mode with detailed step-by-step workflow
                debug_query_execution(orchestrator, args.query)
            else:
                # Normal mode with LangSmith tracing
                print(f" Processando consulta: {args.query}")
                session_id = f"clean_{hash(args.query) % 10000}"
                result = orchestrator.process_query(
                    user_query=args.query,
                    session_id=session_id,
                    streaming=False,
                    run_name=f"clean_query_{session_id}",
                    tags=["production", "txt2sql_agent_clean"],
                    metadata={"script": "txt2sql_agent_clean.py", "mode": "single_query"}
                )
                
                if result["success"]:
                    # Show response
                    print(f" {result['response']}")
                    print(f"   Tempo de execução: {result['execution_time']:.2f}s")
                    
                    # Show SQL for debugging if available
                    if result.get("sql_query"):
                        print(f" SQL: {result['sql_query']}")
                else:
                    print(f" Erro: {result['error_message']}")
                    sys.exit(1)
            return
        
        # Interactive session mode
        if args.debug_steps:
            start_interactive_debug_session(orchestrator)
        else:
            orchestrator.start_interactive_session()
        
    except KeyboardInterrupt:
        print("\n\n Até logo!")
        sys.exit(0)
    
    except Exception as e:
        print(f" Erro fatal: {str(e)}")
        print("\n Dicas para resolução:")
        print("• Verifique se o Ollama está rodando: ollama serve")
        print("• Verifique se o modelo está instalado: ollama pull llama3")
        print("• Verifique se o banco PostgreSQL está acessível: postgresql://postgres:1234@localhost:5432/sih_rs")
        print("• Execute health check: python txt2sql_agent_clean.py --health-check")
        sys.exit(1)


if __name__ == "__main__":
    main()