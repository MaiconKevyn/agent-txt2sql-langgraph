import re
from typing import Tuple


def _strip_sql_comments(sql: str) -> str:
    if not sql:
        return ""
    no_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    no_line = re.sub(r"--.*?$", "", no_block, flags=re.M)
    return no_line


def _strip_string_literals(sql: str) -> str:
    if not sql:
        return ""
    s = sql
    s = re.sub(r"'(?:''|[^'])*'", "''", s)
    s = re.sub(r"\$\$(?:.|\n)*?\$\$", "$$ $$", s)
    s = re.sub(r"\$[a-zA-Z0-9_]*\$(?:.|\n)*?\$[a-zA-Z0-9_]*\$", "$$ $$", s)
    return s


def is_select_only(sql: str) -> Tuple[bool, str]:
    if not sql or not isinstance(sql, str):
        return False, "SQL invalido ou vazio"

    cleaned = _strip_sql_comments(sql).strip()
    if not cleaned:
        return False, "SQL vazio apos remover comentarios"

    tmp = cleaned.rstrip()
    if tmp.endswith(";"):
        tmp = tmp[:-1]

    if ";" in tmp:
        return False, "Multiplas instrucoes SQL nao sao permitidas"

    lowered = _strip_string_literals(tmp).lower().lstrip()

    disallowed = (
        r"\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke|copy|call|do|vacuum|analyze|comment|replace|attach|detach|pragma|refresh|cluster|reindex|checkpoint|checkpointing)\b"
    )
    if re.search(disallowed, lowered):
        return False, "Apenas consultas de leitura (SELECT) sao permitidas"

    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Somente SELECT (ou WITH ... SELECT) e permitido"

    return True, ""


def sanitize_sql(sql: str) -> str:
    if not sql:
        return ""
    cleaned = _strip_sql_comments(sql).strip()
    collapsed = " ".join(cleaned.split())
    # Fix: remove semicolon before LIMIT (common LLM mistake)
    collapsed = re.sub(r";\s*LIMIT", " LIMIT", collapsed, flags=re.IGNORECASE)
    # Fix: remove semicolon before ORDER BY
    collapsed = re.sub(r";\s*ORDER", " ORDER", collapsed, flags=re.IGNORECASE)
    # Fix: remove semicolon before GROUP BY
    collapsed = re.sub(r";\s*GROUP", " GROUP", collapsed, flags=re.IGNORECASE)
    # Remove duplicate semicolons
    while collapsed.endswith(";;"):
        collapsed = collapsed[:-1]
    return collapsed
