from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple


@dataclass
class ExecutionResult:
    rows: List[Sequence[Any]]
    columns: List[str]
    row_count: int
    latency_s: float
    error: Optional[str] = None


class QueryExecutor:
    """
    Database executor that supports both DuckDB and PostgreSQL,
    matching the same connection strategy used by evaluation/dag/tasks.py.
    """

    def __init__(self, database_url: str, statement_timeout_ms: int = 60000):
        self.database_url = database_url
        self.statement_timeout_ms = int(statement_timeout_ms)
        self.is_duckdb = database_url.startswith("duckdb")

        if self.is_duckdb:
            from sqlalchemy import create_engine
            self._engine = create_engine(database_url)
        else:
            import psycopg2
            # Normalize SQLAlchemy format to psycopg2 format
            url = database_url.replace("postgresql+psycopg2://", "postgresql://")
            self.connection = psycopg2.connect(url)

    def execute(self, sql: str) -> ExecutionResult:
        start = time.time()
        if self.is_duckdb:
            return self._execute_duckdb(sql, start)
        return self._execute_postgres(sql, start)

    def _execute_duckdb(self, sql: str, start: float) -> ExecutionResult:
        from sqlalchemy import text
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(sql))
                columns = list(result.keys()) if result.returns_rows else []
                rows = [tuple(row) for row in result.fetchall()] if result.returns_rows else []
            return ExecutionResult(
                rows=rows, columns=columns,
                row_count=len(rows), latency_s=time.time() - start,
            )
        except Exception as exc:
            return ExecutionResult(
                rows=[], columns=[], row_count=0,
                latency_s=time.time() - start, error=str(exc),
            )

    def _execute_postgres(self, sql: str, start: float) -> ExecutionResult:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("BEGIN;")
                cursor.execute(f"SET LOCAL statement_timeout = {self.statement_timeout_ms};")
                cursor.execute(sql)
                columns = [desc[0] for desc in (cursor.description or [])]
                rows = cursor.fetchall() if cursor.description else []
                self.connection.commit()
            return ExecutionResult(
                rows=rows, columns=columns,
                row_count=len(rows), latency_s=time.time() - start,
            )
        except Exception as exc:
            self.connection.rollback()
            return ExecutionResult(
                rows=[], columns=[], row_count=0,
                latency_s=time.time() - start, error=str(exc),
            )

    def execute_query(self, sql: str) -> Tuple[Optional[List[Sequence[Any]]], Optional[str]]:
        """Compatibility method for evaluation metrics."""
        result = self.execute(sql)
        if result.error:
            return None, result.error
        return result.rows, None

    def get_raw_connection(self):
        if self.is_duckdb:
            return self._engine.raw_connection()
        return self.connection

    def close(self) -> None:
        if self.is_duckdb:
            self._engine.dispose()
        elif hasattr(self, "connection") and self.connection:
            self.connection.close()


# Alias mantido para compatibilidade com pipeline.py
PostgresQueryExecutor = QueryExecutor
