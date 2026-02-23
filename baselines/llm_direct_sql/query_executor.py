from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

import psycopg2


@dataclass
class ExecutionResult:
    rows: List[Sequence[Any]]
    columns: List[str]
    row_count: int
    latency_s: float
    error: Optional[str] = None


class PostgresQueryExecutor:
    """
    Thin PostgreSQL wrapper with the same execute_query signature expected by evaluation metrics.
    """

    def __init__(self, database_url: str, statement_timeout_ms: int = 60000):
        self.database_url = database_url
        self.statement_timeout_ms = int(statement_timeout_ms)
        self.connection = psycopg2.connect(database_url)

    def execute(self, sql: str) -> ExecutionResult:
        start = time.time()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("BEGIN;")
                cursor.execute(f"SET LOCAL statement_timeout = {self.statement_timeout_ms};")
                cursor.execute(sql)
                columns = [desc[0] for desc in (cursor.description or [])]
                rows = cursor.fetchall() if cursor.description else []
                self.connection.commit()
            return ExecutionResult(
                rows=rows,
                columns=columns,
                row_count=len(rows),
                latency_s=time.time() - start,
            )
        except Exception as exc:
            self.connection.rollback()
            return ExecutionResult(
                rows=[],
                columns=[],
                row_count=0,
                latency_s=time.time() - start,
                error=str(exc),
            )

    def execute_query(self, sql: str) -> Tuple[Optional[List[Sequence[Any]]], Optional[str]]:
        """
        Compatibility method for existing evaluation metrics.
        """
        result = self.execute(sql)
        if result.error:
            return None, result.error
        return result.rows, None

    def close(self) -> None:
        if self.connection:
            self.connection.close()

