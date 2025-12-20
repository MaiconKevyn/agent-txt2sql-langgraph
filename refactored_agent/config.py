from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class AgentConfig:
    """Configuration for the refactored agent."""

    database_url: str
    llm_provider: str = "ollama"
    llm_model: str = "llama3.1:8b"
    llm_temperature: float = 0.1
    llm_timeout: int = 120

    max_schema_chars: int = 4000
    max_rows: int = 100
    max_results_display: int = 10
    max_retries: int = 2  # Retry SQL generation on validation failure


def load_config(
    database_url: Optional[str] = None,
    llm_provider: Optional[str] = None,
    llm_model: Optional[str] = None,
    llm_temperature: Optional[float] = None,
    llm_timeout: Optional[int] = None,
) -> AgentConfig:
    """Load config from environment and override with explicit args."""

    env_db = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH") or ""
    resolved_db = database_url or env_db
    if not resolved_db:
        raise ValueError("DATABASE_URL not set. Use --db-url or set env.")

    return AgentConfig(
        database_url=resolved_db,
        llm_provider=llm_provider or os.getenv("LLM_PROVIDER", "ollama"),
        llm_model=llm_model or os.getenv("LLM_MODEL", "llama3.1:8b"),
        llm_temperature=(
            float(llm_temperature)
            if llm_temperature is not None
            else float(os.getenv("LLM_TEMPERATURE", "0.1"))
        ),
        llm_timeout=(
            int(llm_timeout)
            if llm_timeout is not None
            else int(os.getenv("LLM_TIMEOUT", "120"))
        ),
    )
