"""
Main Orchestrator V3 - LangGraph Official Patterns

Primary interface for the LangGraph V3 migration following official patterns:
- Easy LLM model switching
- Production-ready SQL Agent
- Complete API compatibility
- Official LangGraph best practices
"""

import time
import logging
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
from datetime import datetime
import json
from dotenv import load_dotenv
import os

# Load environment variables for LangSmith tracing
load_dotenv()

from .workflow import (
    create_production_sql_agent,
    create_development_sql_agent,
    create_testing_sql_agent,
    execute_sql_workflow,
    stream_sql_workflow
)
from .llm_manager import HybridLLMManager
from .state import create_initial_messages_state, state_to_legacy_format
from ..application.config.simple_config import ApplicationConfig, OrchestratorConfig


@dataclass
class ModelConfig:
    """Configuration for LLM model switching"""
    provider: str  # "ollama", "huggingface"
    model_name: str  # "llama3", "mistral", etc.
    temperature: float = 0.1
    timeout: int = 30
    max_retries: int = 3


class LangGraphOrchestrator:
    """
    Main Orchestrator for LangGraph V3 SQL Agent
    
    This is the primary interface that provides:
    - Easy LLM model switching
    - Production-ready SQL Agent workflow
    - Complete API compatibility with legacy system
    - Official LangGraph best practices
    - Performance monitoring and metrics
    """
    
    def __init__(
        self,
        app_config: ApplicationConfig = None,
        orchestrator_config: OrchestratorConfig = None,
        environment: str = "production"
    ):
        """
        Initialize LangGraph Orchestrator
        
        Args:
            app_config: Application configuration
            orchestrator_config: Orchestrator configuration
            environment: "production", "development", or "testing"
        """
        
        # Configuration
        self.app_config = app_config or ApplicationConfig()
        self.orchestrator_config = orchestrator_config or OrchestratorConfig()
        self.environment = environment
        
        # State
        self._workflow = None
        self._llm_manager = None
        self._current_model = None
        self._session_count = 0
        self._total_queries = 0
        self._successful_queries = 0
        self._failed_queries = 0
        self._total_execution_time = 0.0
        
        # Initialize workflow
        self._initialize_workflow()
        
        # Performance tracking
        self._query_history = []
        self._max_history = 1000
        
        # Setup structured logging
        self._setup_logging()
    
    def _initialize_workflow(self):
        """Initialize the appropriate workflow based on environment"""
        try:
            if self.environment == "production":
                self._workflow = create_production_sql_agent()
            elif self.environment == "development":
                self._workflow = create_development_sql_agent()
            elif self.environment == "testing":
                self._workflow = create_testing_sql_agent()
            else:
                # Default to production
                self._workflow = create_production_sql_agent()
            
            # Initialize LLM manager
            self._llm_manager = HybridLLMManager(self.app_config)
            
            # Track current model
            self._current_model = ModelConfig(
                provider=self.app_config.llm_provider,
                model_name=self.app_config.llm_model,
                temperature=self.app_config.llm_temperature,
                timeout=self.app_config.llm_timeout,
                max_retries=self.app_config.llm_max_retries
            )
            
            print(f" LangGraph Orchestrator initialized ({self.environment} mode)")
            print(f"   Model: {self._current_model.model_name} ({self._current_model.provider})")
            
        except Exception as e:
            print(f" Failed to initialize LangGraph Orchestrator: {e}")
            raise
    
    def _setup_logging(self):
        """Setup structured logging for production monitoring"""
        # Create logger for orchestrator
        self.logger = logging.getLogger(f"orchestrator_v3_{self.environment}")
        self.logger.setLevel(logging.INFO if self.environment == "production" else logging.DEBUG)
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            # Console handler with structured format
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # File handler for production
            if self.environment == "production":
                try:
                    file_handler = logging.FileHandler('logs/orchestrator_v3.log')
                    file_handler.setFormatter(formatter)
                    self.logger.addHandler(file_handler)
                except:
                    pass  # Continue without file logging if directory doesn't exist
    
    def switch_model(
        self,
        provider: str,
        model_name: str,
        temperature: float = None,
        timeout: int = None
    ) -> bool:
        """
        Switch to a different LLM model
        
        Args:
            provider: LLM provider ("ollama", "huggingface")
            model_name: Model name ("llama3", "mistral", etc.)
            temperature: Model temperature (optional)
            timeout: Model timeout (optional)
            
        Returns:
            True if switch was successful, False otherwise
        """
        try:
            print(f" Switching model: {model_name} ({provider})")
            
            # Create new configuration
            new_config = ApplicationConfig(
                database_type=self.app_config.database_type,
                database_path=self.app_config.database_path,
                llm_provider=provider,
                llm_model=model_name,
                llm_temperature=temperature or self.app_config.llm_temperature,
                llm_timeout=timeout or self.app_config.llm_timeout,
                llm_max_retries=self.app_config.llm_max_retries,
                llm_device=self.app_config.llm_device,
                llm_load_in_8bit=self.app_config.llm_load_in_8bit,
                llm_load_in_4bit=self.app_config.llm_load_in_4bit,
                schema_type=self.app_config.schema_type,
                ui_type=self.app_config.ui_type,
                interface_type=self.app_config.interface_type,
                error_handling_type=self.app_config.error_handling_type,
                enable_error_logging=self.app_config.enable_error_logging,
                query_processing_type=self.app_config.query_processing_type
            )
            
            # Initialize new LLM manager
            new_llm_manager = HybridLLMManager(new_config)
            
            # Test the new model
            test_result = new_llm_manager.health_check()
            if test_result["status"] != "healthy":
                print(f" Model switch failed: {test_result}")
                return False
            
            # Update configuration and managers
            self.app_config = new_config
            self._llm_manager = new_llm_manager
            
            # Update current model tracking
            self._current_model = ModelConfig(
                provider=provider,
                model_name=model_name,
                temperature=temperature or self.app_config.llm_temperature,
                timeout=timeout or self.app_config.llm_timeout,
                max_retries=self.app_config.llm_max_retries
            )
            
            print(f" Model switched successfully to {model_name} ({provider})")
            return True
            
        except Exception as e:
            print(f" Model switch failed: {e}")
            return False
    
    def process_query(
        self,
        user_query: str,
        session_id: str = None,
        streaming: bool = False,
        config: dict = None,
        run_name: str = None,
        tags: List[str] = None,
        metadata: Dict[str, Any] = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Process a user query using the LangGraph workflow
        
        Args:
            user_query: User's natural language question
            session_id: Optional session identifier
            streaming: Whether to return streaming results
            config: Additional configuration
            run_name: Custom name for LangSmith trace
            tags: Tags for filtering in LangSmith
            metadata: Additional metadata for LangSmith trace
            
        Returns:
            Query result dictionary or list of streaming updates
        """
        start_time = time.time()
        
        # Generate session ID if not provided
        if session_id is None:
            session_id = f"session_{int(time.time() * 1000) % 100000}"
        
        # Log query start
        self.logger.info(f"Query started", extra={
            "query_id": self._total_queries + 1,
            "session_id": session_id,
            "user_query": user_query[:100] + "..." if len(user_query) > 100 else user_query,
            "streaming": streaming,
            "model": f"{self._current_model.provider}/{self._current_model.model_name}"
        })
        
        try:
            # Track query
            self._total_queries += 1
            
            # Prepare LangSmith configuration
            langsmith_config = config or {}
            if run_name:
                langsmith_config["run_name"] = run_name
            if tags:
                langsmith_config["tags"] = tags
            if metadata:
                langsmith_config["metadata"] = metadata
            
            # Add default metadata for tracking
            default_metadata = {
                "session_id": session_id,
                "query_number": self._total_queries,
                "model_provider": self._current_model.provider,
                "model_name": self._current_model.model_name,
                "environment": self.environment
            }
            
            if "metadata" in langsmith_config:
                langsmith_config["metadata"].update(default_metadata)
            else:
                langsmith_config["metadata"] = default_metadata
            
            if streaming:
                # Return streaming results
                results = []
                for update in stream_sql_workflow(
                    workflow=self._workflow,
                    user_query=user_query,
                    session_id=session_id,
                    config=langsmith_config
                ):
                    results.append(update)
                
                # Calculate execution time
                execution_time = time.time() - start_time
                self._total_execution_time += execution_time
                
                # Track success (simplified for streaming)
                self._successful_queries += 1
                
                return results
                
            else:
                # Execute workflow normally
                result = execute_sql_workflow(
                    workflow=self._workflow,
                    user_query=user_query,
                    session_id=session_id,
                    config=langsmith_config
                )
                
                # Calculate execution time
                execution_time = time.time() - start_time
                self._total_execution_time += execution_time
                
                # Update result with actual execution time
                result["execution_time"] = execution_time
                
                # Track success/failure
                if result.get("success", False):
                    self._successful_queries += 1
                    self.logger.info("Query completed successfully", extra={
                        "query_id": self._total_queries,
                        "session_id": session_id,
                        "execution_time": execution_time,
                        "sql_query": result.get("sql_query", "")[:100] + "..." if result.get("sql_query") and len(result.get("sql_query", "")) > 100 else result.get("sql_query", ""),
                        "row_count": len(result.get("results", []))
                    })
                else:
                    self._failed_queries += 1
                    self.logger.error("Query failed", extra={
                        "query_id": self._total_queries,
                        "session_id": session_id,
                        "execution_time": execution_time,
                        "error_message": result.get("error_message", "Unknown error")
                    })
                
                # Add to query history
                self._add_to_history(user_query, result, execution_time)
                
                # Enhance result with orchestrator metadata
                result["metadata"] = result.get("metadata", {})
                result["metadata"].update({
                    "orchestrator_v3": True,
                    "current_model": {
                        "provider": self._current_model.provider,
                        "model_name": self._current_model.model_name,
                        "temperature": self._current_model.temperature
                    },
                    "environment": self.environment,
                    "session_id": session_id,
                    "query_number": self._total_queries,
                    "orchestrator_execution_time": execution_time
                })
                
                return result
                
        except Exception as e:
            # Handle orchestrator-level errors
            execution_time = time.time() - start_time
            self._total_execution_time += execution_time
            self._failed_queries += 1
            
            error_result = {
                "success": False,
                "question": user_query,
                "sql_query": None,
                "results": [],
                "row_count": 0,
                "execution_time": execution_time,
                "error_message": f"Orchestrator error: {str(e)}",
                "response": f"Erro do sistema: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "metadata": {
                    "orchestrator_v3": True,
                    "orchestrator_error": True,
                    "error_type": "orchestrator_execution_error",
                    "current_model": {
                        "provider": self._current_model.provider,
                        "model_name": self._current_model.model_name
                    },
                    "environment": self.environment
                }
            }
            
            return error_result
    
    def _add_to_history(self, query: str, result: dict, execution_time: float):
        """Add query to history for performance tracking"""
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "query": query[:100],  # Truncate for memory
            "success": result.get("success", False),
            "execution_time": execution_time,
            "model": f"{self._current_model.provider}/{self._current_model.model_name}",
            "error": result.get("error_message") if not result.get("success") else None
        }
        
        self._query_history.append(history_entry)
        
        # Maintain history limit
        if len(self._query_history) > self._max_history:
            self._query_history = self._query_history[-self._max_history:]
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics
        
        Returns:
            Dictionary with performance statistics
        """
        
        avg_execution_time = (
            self._total_execution_time / self._total_queries 
            if self._total_queries > 0 else 0
        )
        
        success_rate = (
            self._successful_queries / self._total_queries 
            if self._total_queries > 0 else 0
        )
        
        # Recent performance (last 10 queries)
        recent_queries = self._query_history[-10:] if self._query_history else []
        recent_success_rate = (
            sum(1 for q in recent_queries if q["success"]) / len(recent_queries)
            if recent_queries else 0
        )
        
        recent_avg_time = (
            sum(q["execution_time"] for q in recent_queries) / len(recent_queries)
            if recent_queries else 0
        )
        
        return {
            "orchestrator_info": {
                "version": "3.0",
                "environment": self.environment,
                "current_model": {
                    "provider": self._current_model.provider,
                    "model_name": self._current_model.model_name,
                    "temperature": self._current_model.temperature
                }
            },
            "total_statistics": {
                "total_queries": self._total_queries,
                "successful_queries": self._successful_queries,
                "failed_queries": self._failed_queries,
                "success_rate": success_rate,
                "average_execution_time": avg_execution_time,
                "total_execution_time": self._total_execution_time
            },
            "recent_performance": {
                "recent_queries_count": len(recent_queries),
                "recent_success_rate": recent_success_rate,
                "recent_average_time": recent_avg_time
            },
            "model_performance": self._get_model_performance(),
            "llm_manager_health": self._llm_manager.health_check() if self._llm_manager else {"status": "unavailable"}
        }
    
    def _get_model_performance(self) -> Dict[str, Any]:
        """Get performance breakdown by model"""
        model_stats = {}
        
        for entry in self._query_history:
            model = entry["model"]
            if model not in model_stats:
                model_stats[model] = {
                    "queries": 0,
                    "successes": 0,
                    "total_time": 0.0
                }
            
            stats = model_stats[model]
            stats["queries"] += 1
            if entry["success"]:
                stats["successes"] += 1
            stats["total_time"] += entry["execution_time"]
        
        # Calculate derived metrics
        for model, stats in model_stats.items():
            stats["success_rate"] = stats["successes"] / stats["queries"] if stats["queries"] > 0 else 0
            stats["average_time"] = stats["total_time"] / stats["queries"] if stats["queries"] > 0 else 0
        
        return model_stats
    
    def get_available_models(self) -> Dict[str, List[str]]:
        """
        Get list of available models by provider
        
        Returns:
            Dictionary mapping providers to available models
        """
        return {
            "ollama": ["llama3", "llama3.2", "mistral"],
            "huggingface": [
                "microsoft/DialoGPT-medium",
                "microsoft/DialoGPT-large", 
                "EleutherAI/gpt-neo-2.7B",
                "EleutherAI/gpt-j-6B"
            ]
        }
    
    def get_current_model(self) -> Dict[str, Any]:
        """Get current model information"""
        if not self._llm_manager:
            return {"error": "LLM manager not initialized"}
        
        model_info = self._llm_manager.get_model_info()
        model_info.update({
            "orchestrator_config": {
                "provider": self._current_model.provider,
                "model_name": self._current_model.model_name,
                "temperature": self._current_model.temperature,
                "timeout": self._current_model.timeout,
                "max_retries": self._current_model.max_retries
            }
        })
        
        return model_info
    
    def get_workflow_visualization(self, xray: bool = True) -> bytes:
        """
        Generate workflow visualization using LangGraph's built-in method
        
        Args:
            xray: Enable detailed view with internal state information
            
        Returns:
            PNG bytes of the workflow diagram
        """
        if not self._workflow:
            raise ValueError("Workflow not initialized")
        
        # Use the same pattern from LangGraph docs
        return self._workflow.get_graph(xray=xray).draw_mermaid_png()

    def display_workflow(self, xray: bool = True):
        """
        Display workflow in Jupyter notebook (same as docs example)
        
        Args:
            xray: Enable detailed view with internal state information
        """
        try:
            from IPython.display import Image, display
            # Exact same pattern as documentation
            display(Image(self._workflow.get_graph(xray=xray).draw_mermaid_png()))
        except ImportError:
            print("IPython not available. Use save_workflow_diagram() instead.")

    def save_workflow_diagram(self, filename: str = "workflow.png", xray: bool = True):
        """
        Save workflow diagram to file
        
        Args:
            filename: Output filename for the PNG diagram
            xray: Enable detailed view with internal state information
        """
        try:
            png_data = self.get_workflow_visualization(xray=xray)
            with open(filename, "wb") as f:
                f.write(png_data)
            print(f" Workflow diagram saved to {filename}")
        except Exception as e:
            print(f" Failed to save workflow diagram: {str(e)}")

    def print_workflow_structure(self):
        """Print text representation of workflow structure"""
        if not self._workflow:
            print(" Workflow not initialized")
            return
        
        print("LangGraph Text2SQL Workflow Structure:")
        print("=" * 60)
        
        workflow_structure = """
    START
      ↓
     query_classification_node
      ↓
    [Route based on classification]
      ↓
    DATABASE Route:
      ↓
     list_tables_node (discover available tables)
      ↓  
     get_schema_node (retrieve table schemas)
      ↓
     generate_sql_node (generate SQL query)
      ↓
     validate_sql_node (validate SQL syntax)
      ↓
     execute_sql_node (execute query on database)
      ↓
     generate_response_node (format final response)
      ↓
    END
    
    CONVERSATIONAL Route:
      ↓
     generate_response_node (direct conversational response)
      ↓
    END
    
    SCHEMA Route:
      ↓
     list_tables_node (table discovery only)
      ↓
     generate_response_node (schema information response)
      ↓
    END
    
     Features:
    • PostgreSQL with 15 specialized tables
    • Intelligent table selection (mortes, procedimentos, etc.)
    • Multi-LLM support (Ollama, HuggingFace)
    • Retry mechanisms with error recovery
    • Healthcare domain optimization (SUS data)
        """
        
        print(workflow_structure)

    def process_query_with_tracing(
        self,
        user_query: str,
        session_id: str = None,
        run_name: str = None,
        project_name: str = "txt2sql-agent"
    ) -> Dict[str, Any]:
        """
        Process a query with enhanced LangSmith tracing
        
        Args:
            user_query: User's natural language question
            session_id: Optional session identifier  
            run_name: Custom name for the trace
            project_name: LangSmith project name
            
        Returns:
            Query result with enhanced tracing metadata
        """
        # Generate meaningful run name if not provided
        if not run_name:
            run_name = f"txt2sql_query_{self._total_queries + 1}"
        
        # Create comprehensive tags
        tags = [
            f"model:{self._current_model.model_name}",
            f"provider:{self._current_model.provider}",
            f"env:{self.environment}",
            "txt2sql",
            "langgraph-v3"
        ]
        
        # Enhanced metadata for debugging
        metadata = {
            "project": project_name,
            "orchestrator_version": "3.0",
            "user_query_length": len(user_query),
            "user_query_preview": user_query[:100] + "..." if len(user_query) > 100 else user_query
        }
        
        # Execute with tracing
        return self.process_query(
            user_query=user_query,
            session_id=session_id,
            run_name=run_name,
            tags=tags,
            metadata=metadata
        )

    def health_check(self) -> Dict[str, Any]:
        """
        Comprehensive health check for the orchestrator
        
        Returns:
            Health status dictionary
        """
        try:
            # Check LLM manager health
            llm_health = self._llm_manager.health_check() if self._llm_manager else {"status": "failed"}
            
            # Check workflow status
            workflow_status = "healthy" if self._workflow else "failed"
            
            # Overall status
            overall_status = "healthy" if (
                llm_health.get("status") == "healthy" and 
                workflow_status == "healthy"
            ) else "degraded"
            
            return {
                "status": overall_status,
                "timestamp": datetime.now().isoformat(),
                "orchestrator": {
                    "version": "3.0",
                    "environment": self.environment,
                    "workflow_status": workflow_status,
                    "total_queries": self._total_queries,
                    "success_rate": (
                        self._successful_queries / self._total_queries 
                        if self._total_queries > 0 else 0
                    )
                },
                "llm_manager": llm_health,
                "current_model": {
                    "provider": self._current_model.provider,
                    "model_name": self._current_model.model_name,
                    "available": llm_health.get("status") == "healthy"
                }
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "orchestrator": {"version": "3.0", "error": True}
            }
    
    def reset_metrics(self):
        """Reset performance metrics"""
        self._total_queries = 0
        self._successful_queries = 0
        self._failed_queries = 0
        self._total_execution_time = 0.0
        self._query_history = []
        print(" Performance metrics reset")
    
    def __str__(self) -> str:
        """String representation of orchestrator"""
        return (
            f"LangGraphOrchestrator(v3.0, {self.environment}, "
            f"{self._current_model.model_name}@{self._current_model.provider}, "
            f"queries={self._total_queries})"
        )


# Factory functions for easy instantiation
def create_orchestrator(
    provider: str = "ollama",
    model_name: str = "llama3",
    environment: str = "production",
    database_path: str = "sus_database.db"
) -> LangGraphOrchestrator:
    """
    Factory function to create LangGraph Orchestrator
    
    Args:
        provider: LLM provider ("ollama", "huggingface")
        model_name: Model name
        environment: Environment mode
        database_path: Path to database
        
    Returns:
        Configured LangGraphOrchestrator instance
    """
    
    app_config = ApplicationConfig(
        database_path=database_path,
        llm_provider=provider,
        llm_model=model_name
    )
    
    orchestrator_config = OrchestratorConfig()
    
    return LangGraphOrchestrator(
        app_config=app_config,
        orchestrator_config=orchestrator_config,
        environment=environment
    )


def create_production_orchestrator(
    provider: str = "ollama",
    model_name: str = "llama3"
) -> LangGraphOrchestrator:
    """Create production-ready orchestrator"""
    return create_orchestrator(
        provider=provider,
        model_name=model_name,
        environment="production"
    )


def create_development_orchestrator(
    provider: str = "ollama",
    model_name: str = "llama3"
) -> LangGraphOrchestrator:
    """Create development orchestrator with debugging"""
    return create_orchestrator(
        provider=provider,
        model_name=model_name,
        environment="development"
    )


# Export main classes and functions
__all__ = [
    "LangGraphOrchestrator",
    "ModelConfig",
    "create_orchestrator",
    "create_production_orchestrator", 
    "create_development_orchestrator"
]