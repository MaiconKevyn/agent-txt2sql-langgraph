from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

from baselines.llm_direct_sql.config import BaselineConfig

try:
    from langchain_groq import ChatGroq

    GROQ_AVAILABLE = True
except Exception:
    GROQ_AVAILABLE = False


@dataclass
class LLMOutput:
    raw_text: str
    latency_s: float
    provider: str
    model: str


def _normalize_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
                elif "content" in item:
                    parts.append(str(item["content"]))
            else:
                parts.append(str(item))
        return "\n".join(parts).strip()
    return str(content).strip()


class DirectLLMClient:
    def __init__(self, config: BaselineConfig):
        self.config = config
        self._llm = self._build_llm()

    def _build_llm(self):
        provider = self.config.llm_provider.lower()
        if provider == "ollama":
            return ChatOllama(
                model=self.config.llm_model,
                temperature=self.config.llm_temperature,
                timeout=self.config.llm_timeout,
            )
        if provider == "openai":
            return ChatOpenAI(
                model=self.config.llm_model,
                temperature=self.config.llm_temperature,
                timeout=self.config.llm_timeout,
            )
        if provider == "groq":
            if not GROQ_AVAILABLE:
                raise ImportError(
                    "Provider 'groq' requested but langchain_groq is not installed."
                )
            return ChatGroq(
                model=self.config.llm_model,
                temperature=self.config.llm_temperature,
                timeout=self.config.llm_timeout,
            )
        raise ValueError(
            f"Unsupported provider '{self.config.llm_provider}'. "
            "Supported: ollama, openai, groq"
        )

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMOutput:
        start = time.time()
        response = self._llm.invoke(
            [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        )
        latency = time.time() - start

        if isinstance(response, AIMessage):
            text = _normalize_content(response.content)
        else:
            text = _normalize_content(response)

        return LLMOutput(
            raw_text=text,
            latency_s=latency,
            provider=self.config.llm_provider,
            model=self.config.llm_model,
        )

