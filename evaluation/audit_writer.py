"""
audit_writer.py — Serializa audit_results.json com truncamento a 100 linhas.
"""
from __future__ import annotations

import json
from pathlib import Path

from evaluation.query_executor import ExecutionResult


def build_audit_entry(
    entry_id: str,
    question: str,
    gt_sql: str,
    agent_sql: str,
    gt_result: ExecutionResult,
    agent_result: ExecutionResult,
    status: str,  # "PASS" | "FAIL" | "ERROR"
) -> dict:
    """Constrói um dict JSON-safe para o audit."""

    def _serialize_result(r: ExecutionResult) -> dict:
        return {
            "columns": r.columns,
            "rows": [list(row) for row in r.rows],
            "total_rows": r.total_rows,
            "truncated": r.truncated,
        }

    return {
        "id": entry_id,
        "question": question,
        "ground_truth_sql": gt_sql,
        "agent_sql": agent_sql,
        "gt_output": _serialize_result(gt_result),
        "agent_output": _serialize_result(agent_result),
        "status": status,
        "gt_error": gt_result.error,
        "agent_error": agent_result.error,
    }


def write_audit(path: Path, entries: list[dict]) -> None:
    """Escreve audit_results.json."""
    path.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
