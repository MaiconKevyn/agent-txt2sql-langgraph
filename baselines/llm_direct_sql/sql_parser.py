from __future__ import annotations

import re
from dataclasses import dataclass

from src.utils.sql_safety import is_select_only, sanitize_sql_for_execution


_SQL_BLOCK_PATTERN = re.compile(r"```sql\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)
_GENERIC_BLOCK_PATTERN = re.compile(r"```\s*(.*?)```", flags=re.DOTALL)
_SQL_PREFIX_PATTERN = re.compile(r"^\s*(sqlquery|sql)\s*:\s*", flags=re.IGNORECASE)


@dataclass
class ParsedSQL:
    sql: str
    is_safe: bool
    safety_reason: str


def extract_sql(raw_text: str) -> str:
    if not raw_text:
        return ""

    sql_text = raw_text.strip()

    sql_block = _SQL_BLOCK_PATTERN.search(sql_text)
    if sql_block:
        sql_text = sql_block.group(1).strip()
    else:
        generic_block = _GENERIC_BLOCK_PATTERN.search(sql_text)
        if generic_block:
            sql_text = generic_block.group(1).strip()

    sql_text = _SQL_PREFIX_PATTERN.sub("", sql_text).strip()
    return sanitize_sql_for_execution(sql_text)


def parse_and_validate_sql(raw_text: str) -> ParsedSQL:
    sql = extract_sql(raw_text)
    safe, reason = is_select_only(sql)
    return ParsedSQL(sql=sql, is_safe=safe, safety_reason=reason)

