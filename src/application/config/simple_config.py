from dataclasses import dataclass
from typing import Optional
from enum import Enum

class InterfaceType(Enum):
    """Interface type configuration"""
    CLI_BASIC = "cli_basic"
    CLI_INTERACTIVE = "cli_interactive"


@dataclass
class ApplicationConfig:
    """Simple configuration for the application"""
    # Database configuration
    database_type: str = "postgresql"
    database_path: str = "postgresql+psycopg2://postgres:1234@localhost:5432/sih_rs"
    
    # LLM configuration (for SQL generation)
    # llm_provider: str = "huggingface"  # ollama, huggingface
    # llm_model: str = "maiconkevyn/mistral-txt2sql-sus"  # Fine-tuned SUS model
    llm_provider: str = "ollama"  # Fallback to Ollama
    llm_model: str = "llama3.1:8b"  # llama3.1:8b with tool calling support
    # llm_model: str = "defog/sqlcoder-7b-2"  # Heavy model - causes IDE crashes
    llm_temperature: float = 0.1  # Optimized for fine-tuned model
    llm_timeout: int = 120
    llm_max_retries: int = 3
    
    # Conversational LLM configuration (for natural language responses)
    conversational_llm_model: str = "llama3.1:8b"  # llama3.1:8b, mistral
    conversational_llm_temperature: float = 0.8
    conversational_llm_max_tokens: int = 1000
    conversational_llm_timeout: int = 60
    conversational_llm_max_retries: int = 3
    
    # HuggingFace model configuration
    llm_device: str = "auto"  # auto detects best device (cuda/cpu)
    llm_load_in_8bit: bool = False
    llm_load_in_4bit: bool = True  # Enable 4-bit quantization for efficiency
    llm_max_new_tokens: int = 300  # Increased for better SQL generation
    
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
    enable_query_classification: bool = False
    query_classification_confidence_threshold: float = 0.7

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