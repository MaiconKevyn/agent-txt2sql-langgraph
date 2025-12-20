import logging
from typing import List, Dict, Any, Tuple

from sqlalchemy import create_engine, text, inspect

logger = logging.getLogger("refactored_agent.db")


class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.inspector = inspect(self.engine)

    def list_tables(self) -> List[str]:
        return self.inspector.get_table_names()

    def get_schema_text(self, tables: List[str]) -> str:
        lines: List[str] = []
        for table in tables:
            lines.append(f"TABLE {table}:")
            try:
                columns = self.inspector.get_columns(table)
            except Exception:
                columns = []
            for col in columns:
                col_name = col.get("name", "")
                col_type = str(col.get("type", ""))
                lines.append(f"- {col_name} ({col_type})")
            lines.append("")
        return "\n".join(lines).strip()

    def explain(self, sql: str) -> Tuple[bool, str]:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"EXPLAIN {sql}"))
            return True, ""
        except Exception as exc:
            return False, str(exc)

    def execute(self, sql: str, max_rows: int = 100) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            try:
                rows = result.mappings().fetchmany(size=max_rows)
                return [dict(row) for row in rows]
            except Exception:
                return []

    def ping(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            logger.error("Database ping failed", extra={"error": str(exc)})
            return False
