from typing import List
import logging

from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage, AIMessage

logger = logging.getLogger("refactored_agent.llm")


class LLMClient:
    def __init__(self, provider: str, model: str, temperature: float, timeout: int):
        self.provider = provider.lower()
        self.model = model
        self.temperature = temperature
        self.timeout = timeout
        self._llm = self._init_llm()

    def _init_llm(self):
        if self.provider == "ollama":
            from langchain_ollama import ChatOllama

            return ChatOllama(
                model=self.model,
                temperature=self.temperature,
                timeout=self.timeout,
                num_predict=1024,
                top_k=5,
                top_p=0.9,
            )

        if self.provider in ("huggingface", "hf"):
            from langchain_community.llms import HuggingFacePipeline

            return HuggingFacePipeline.from_model_id(
                model_id=self.model,
                task="text-generation",
                device=-1,
                pipeline_kwargs={
                    "max_new_tokens": 300,
                    "temperature": self.temperature,
                    "do_sample": self.temperature > 0,
                },
            )

        raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def _messages_to_prompt(self, messages: List[BaseMessage]) -> str:
        parts = []
        for msg in messages:
            content = getattr(msg, "content", "")
            if isinstance(msg, SystemMessage):
                prefix = "SYSTEM: "
            elif isinstance(msg, HumanMessage):
                prefix = "USER: "
            elif isinstance(msg, AIMessage):
                prefix = "ASSISTANT: "
            else:
                prefix = ""
            parts.append(f"{prefix}{content}")
        return "\n\n".join(parts)

    def invoke(self, messages: List[BaseMessage]) -> str:
        if self.provider in ("huggingface", "hf"):
            prompt = self._messages_to_prompt(messages)
            response = self._llm.invoke(prompt)
            return response if isinstance(response, str) else str(response)

        response = self._llm.invoke(messages)
        return response.content if hasattr(response, "content") else str(response)
