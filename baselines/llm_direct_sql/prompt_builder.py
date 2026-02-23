from __future__ import annotations

from typing import Tuple


SYSTEM_PROMPT = """You are a PostgreSQL text-to-SQL assistant for the sihrd5 healthcare database.

Generate exactly one SQL query for the user question.

Mandatory rules:
1) Output SQL only. No markdown. No explanations.
2) Produce read-only SQL: SELECT or WITH ... SELECT only.
3) Use only tables and columns present in the provided schema context.
4) Prefer explicit JOIN conditions and valid PostgreSQL syntax.
5) Use double quotes for case-sensitive column names.
"""


def build_prompts(question: str, schema_context: str) -> Tuple[str, str]:
    user_prompt = f"""Schema context:
{schema_context}

User question:
{question}
"""
    return SYSTEM_PROMPT, user_prompt

