from __future__ import annotations

import time
from dataclasses import dataclass

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from baselines.rich_prompt_baseline.config import BaselineConfig


@dataclass
class LLMOutput:
    raw_text: str
    latency_s: float
    model: str


class DirectLLMClient:
    def __init__(self, config: BaselineConfig):
        self.config = config
        self._llm = ChatOpenAI(
            model=config.llm_model,
            temperature=config.llm_temperature,
            timeout=config.llm_timeout,
        )

    def generate_sql(self, system_prompt: str, user_prompt: str) -> LLMOutput:
        from openai import RateLimitError
        messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
        max_retries = 8
        wait = 5.0
        start = time.time()
        for attempt in range(max_retries):
            try:
                response = self._llm.invoke(messages)
                latency = time.time() - start
                text = response.content.strip() if isinstance(response, AIMessage) else str(response).strip()
                return LLMOutput(raw_text=text, latency_s=latency, model=self.config.llm_model)
            except RateLimitError as e:
                if attempt == max_retries - 1:
                    raise
                print(f"          [rate limit] waiting {wait:.0f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                wait = min(wait * 2, 60.0)
