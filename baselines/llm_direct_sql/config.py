from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


def _first_non_empty(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return None


def _as_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class BaselineConfig:
    llm_provider: str
    llm_model: str
    llm_temperature: float
    llm_timeout: int
    database_url: str
    statement_timeout_ms: int
    include_raw_response: bool
    output_dir: Path

    @classmethod
    def from_env(
        cls,
        *,
        llm_provider: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
        llm_timeout: Optional[int] = None,
        database_url: Optional[str] = None,
        statement_timeout_ms: Optional[int] = None,
        include_raw_response: Optional[bool] = None,
        output_dir: Optional[str] = None,
    ) -> "BaselineConfig":
        provider = _first_non_empty(
            llm_provider,
            os.getenv("BASELINE_LLM_PROVIDER"),
            os.getenv("LLM_PROVIDER"),
            "ollama",
        )
        model = _first_non_empty(
            llm_model,
            os.getenv("BASELINE_LLM_MODEL"),
            os.getenv("LLM_MODEL"),
            "llama3.1:8b",
        )

        temp = (
            llm_temperature
            if llm_temperature is not None
            else float(os.getenv("BASELINE_LLM_TEMPERATURE", "0"))
        )
        timeout = (
            llm_timeout
            if llm_timeout is not None
            else int(os.getenv("BASELINE_LLM_TIMEOUT", "120"))
        )

        db_url = _first_non_empty(
            database_url,
            os.getenv("BASELINE_DATABASE_URL"),
            os.getenv("DATABASE_URL"),
            os.getenv("DATABASE_PATH"),
        )
        if not db_url:
            raise ValueError("DATABASE_URL (or BASELINE_DATABASE_URL) is required")

        normalized_db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")

        stmt_timeout = (
            statement_timeout_ms
            if statement_timeout_ms is not None
            else int(os.getenv("BASELINE_STATEMENT_TIMEOUT_MS", "60000"))
        )

        raw_response = (
            include_raw_response
            if include_raw_response is not None
            else _as_bool(os.getenv("BASELINE_INCLUDE_RAW_RESPONSE"), default=False)
        )

        out_dir = Path(
            output_dir
            or os.getenv("BASELINE_OUTPUT_DIR")
            or "baselines/llm_direct_sql/artifacts"
        )

        return cls(
            llm_provider=provider,
            llm_model=model,
            llm_temperature=float(temp),
            llm_timeout=int(timeout),
            database_url=normalized_db_url,
            statement_timeout_ms=int(stmt_timeout),
            include_raw_response=bool(raw_response),
            output_dir=out_dir,
        )

