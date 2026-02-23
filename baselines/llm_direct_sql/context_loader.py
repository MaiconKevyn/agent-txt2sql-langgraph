from __future__ import annotations

from typing import Dict, List

from src.application.config.table_descriptions import TABLE_DESCRIPTIONS


def _join_items(items: List[str], limit: int) -> str:
    if not items:
        return "-"
    trimmed = items[:limit]
    return ", ".join(trimmed)


def build_schema_context(
    table_descriptions: Dict = TABLE_DESCRIPTIONS,
    *,
    max_key_columns: int = 12,
    max_notes: int = 5,
) -> str:
    """
    Build concise schema context for direct LLM prompting.
    """
    blocks: List[str] = []
    for table_name in sorted(table_descriptions.keys()):
        info = table_descriptions.get(table_name, {})
        title = info.get("title", table_name)
        description = info.get("description", "").strip()
        key_columns = info.get("key_columns", [])
        critical_notes = info.get("critical_notes", [])
        relationships = info.get("relationships", [])

        block = "\n".join(
            [
                f"TABLE: {table_name}",
                f"TITLE: {title}",
                f"DESCRIPTION: {description or '-'}",
                f"KEY_COLUMNS: {_join_items(key_columns, max_key_columns)}",
                f"CRITICAL_NOTES: {_join_items(critical_notes, max_notes)}",
                f"RELATIONSHIPS: {_join_items(relationships, max_notes)}",
            ]
        )
        blocks.append(block)

    return "\n\n".join(blocks)

