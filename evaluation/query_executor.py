"""
query_executor.py — Executa SQL no banco e retorna rows + metadados de audit.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.connection import get_connection

MAX_AUDIT_ROWS = 100


@dataclass
class ExecutionResult:
    rows: list[tuple]       # até MAX_AUDIT_ROWS linhas (para audit)
    columns: list[str]
    total_rows: int         # total real retornado pelo banco
    truncated: bool         # True se total_rows > MAX_AUDIT_ROWS
    error: str | None


def execute_sql(sql: str) -> ExecutionResult:
    """Executa SQL no banco (read-only) e retorna ExecutionResult."""
    try:
        with get_connection(read_only=True) as conn:
            result = conn.execute(sql)
            all_rows = result.fetchall()
            columns = [desc[0] for desc in result.description] if result.description else []

        total = len(all_rows)
        truncated = total > MAX_AUDIT_ROWS
        rows = all_rows[:MAX_AUDIT_ROWS]

        return ExecutionResult(
            rows=rows,
            columns=columns,
            total_rows=total,
            truncated=truncated,
            error=None,
        )
    except Exception as e:
        return ExecutionResult(
            rows=[],
            columns=[],
            total_rows=0,
            truncated=False,
            error=str(e),
        )
