import re
from typing import Tuple


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments (/* ... */ and -- ... EOL)."""
    if not sql:
        return ""
    # Remove block comments
    no_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    # Remove line comments
    no_line = re.sub(r"--.*?$", "", no_block, flags=re.M)
    return no_line


def is_select_only(sql: str) -> Tuple[bool, str]:
    """
    Check if the SQL statement is read-only (SELECT/CTE only).

    Rules:
    - Allow statements that start with SELECT or WITH (CTE) leading to SELECT
    - Disallow common DDL/DML and administrative commands
    - Disallow multiple statements (more than one semicolon)

    Returns (ok, reason_when_false)
    """
    if not sql or not isinstance(sql, str):
        return False, "SQL inválido ou vazio"

    # Normalize and strip comments/whitespace
    cleaned = _strip_sql_comments(sql).strip()
    if not cleaned:
        return False, "SQL vazio após remover comentários"

    # Normalize trailing semicolon: permit only a single trailing ';'
    tmp = cleaned.rstrip()
    if tmp.endswith(";"):
        tmp = tmp[:-1]

    # Reject multiple statements (another ';' remains)
    if ";" in tmp:
        return False, "Múltiplas instruções SQL não são permitidas"

    # Lowercase for keyword checks
    lowered = tmp.lower().lstrip()

    # Disallowed keywords (word boundary to avoid matching column/table names)
    disallowed = (
        r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke|copy|call|do|vacuum|analyze|comment|replace|attach|detach|pragma|refresh|cluster|reindex|checkpoint|checkpointing)\b"
    )
    if re.search(disallowed, lowered):
        return False, "Apenas consultas de leitura (SELECT) são permitidas"

    # Must start with SELECT or WITH (CTE)
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Somente SELECT (ou WITH ... SELECT) é permitido"

    return True, ""


def sanitize_sql_for_execution(sql: str) -> str:
    """
    Produce a "clean" SQL string safe for validation/execution:
    - Remove comments (/* ... */ and -- ... EOL)
    - Trim surrounding whitespace
    - Collapse excessive internal whitespace and line breaks
    - Keep, at most, a single trailing semicolon
    """
    if not sql:
        return ""
    cleaned = _strip_sql_comments(sql).strip()
    # Collapse whitespace/newlines to single spaces
    collapsed = " ".join(cleaned.split())
    # Normalize trailing semicolon: allow at most one
    while collapsed.endswith(";;"):
        collapsed = collapsed[:-1]
    return collapsed
