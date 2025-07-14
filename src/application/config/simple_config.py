"""
Simple Configuration - Replace dependency injection with direct configuration
"""
from dataclasses import dataclass
from typing import Optional
from ..services.user_interface_service import InterfaceType


@dataclass
class ApplicationConfig:
    """Simple configuration for the application"""
    # Database configuration
    database_type: str = "sqlite"
    database_path: str = "sus_database.db"
    
    # LLM configuration
    # llm_provider: str = "huggingface"  # ollama, huggingface
    # llm_model: str = "defog/sqlcoder-7b-2"  # llama3, defog/sqlcoder-7b-2
    llm_provider: str = "ollama"  # ollama, huggingface
    llm_model: str = "mistral"  # llama3, defog/sqlcoder-7b-2
    llm_temperature: float = 0.0
    llm_timeout: int = 120
    llm_max_retries: int = 3
    
    # Hugging Face specific configuration
    llm_device: str = "auto"  # auto, cpu, cuda
    llm_load_in_8bit: bool = False
    llm_load_in_4bit: bool = True  # Recommended for SQLCoder-7b-2
    
    # Schema configuration
    schema_type: str = "sus"
    
    # UI configuration
    ui_type: str = "cli"
    interface_type: InterfaceType = InterfaceType.CLI_INTERACTIVE
    
    # Error handling configuration
    error_handling_type: str = "comprehensive"
    enable_error_logging: bool = True
    
    # Query processing configuration
    query_processing_type: str = "comprehensive"
    
    # Query classification configuration
    enable_query_classification: bool = True
    query_classification_confidence_threshold: float = 0.7
    
    # CID configuration
    cid_repository_type: str = "sqlite"
    enable_cid_semantic_search: bool = True


@dataclass
class OrchestratorConfig:
    """Configuration for the orchestrator"""
    max_query_length: int = 1000
    enable_query_history: bool = True
    enable_statistics: bool = True
    session_timeout: int = 3600  # seconds
    enable_conversational_response: bool = True
    conversational_fallback: bool = True
    enable_query_routing: bool = True
    routing_confidence_threshold: float = 0.7
    # Simple Query Decomposition Configuration
    enable_query_decomposition: bool = True
    decomposition_complexity_threshold: int = 2
    decomposition_timeout_seconds: float = 60.0
    decomposition_fallback_enabled: bool = True
    decomposition_debug_mode: bool = True