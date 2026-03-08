"""
evaluator.py — Orquestra: carrega GT → gera agent_sql → executa → compara.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from evaluation.audit_writer import build_audit_entry
from evaluation.query_executor import ExecutionResult, execute_sql
from evaluation.result_comparator import is_subset
from src.text2sql.pipeline import Text2SQLPipeline


@dataclass
class GroundTruthEntry:
    id: str
    difficulty: str
    question: str
    ground_truth_sql: str   # mapeado de "query" no JSON
    tables: list[str]
    notes: str


@dataclass
class EvalRecord:
    id: str
    question: str
    difficulty: str
    ground_truth_sql: str
    agent_sql: str
    passed: bool
    gt_error: str | None
    agent_error: str | None
    gt_total_rows: int
    agent_total_rows: int
    latency_ms: int         # latência do pipeline (geração agent_sql)
    repair_attempts: int


def load_ground_truth(path: Path) -> list[GroundTruthEntry]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [
        GroundTruthEntry(
            id=item["id"],
            difficulty=item["difficulty"],
            question=item["question"],
            ground_truth_sql=item["query"],
            tables=item.get("tables", []),
            notes=item.get("notes", ""),
        )
        for item in data
    ]


def _render_progress(current: int, total: int, n_pass: int, n_fail: int, n_error: int) -> str:
    bar_width = 28
    filled = int(bar_width * current / total) if total else 0
    bar = "█" * filled + "░" * (bar_width - filled)
    pct = current / total * 100 if total else 0.0
    return (
        f"\r[{bar}] {current}/{total} ({pct:5.1f}%)"
        f"  PASS: {n_pass}"
        f"  FAIL: {n_fail}"
        f"  ERROR: {n_error}  "
    )


def run_evaluation(
    gt_entries: list[GroundTruthEntry],
    pipeline: Text2SQLPipeline,
) -> tuple[list[EvalRecord], list[dict]]:
    """
    Executa avaliação sobre todas as entradas do ground truth.

    Returns:
        (records, audit_entries)
    """
    records: list[EvalRecord] = []
    audit_entries: list[dict] = []

    _empty_result = ExecutionResult(rows=[], columns=[], total_rows=0, truncated=False, error=None)
    total = len(gt_entries)
    n_pass = n_fail = n_error = 0

    print(_render_progress(0, total, 0, 0, 0), end="", flush=True)

    for i, entry in enumerate(gt_entries, 1):
        # 1. Gera agent_sql via pipeline
        t0 = time.monotonic()
        pipe_result = pipeline.run(entry.question)
        latency_ms = int((time.monotonic() - t0) * 1000)

        agent_sql = pipe_result.sql
        agent_error: str | None = None if pipe_result.success else pipe_result.error

        # 2. Executa ground truth SQL
        gt_result = execute_sql(entry.ground_truth_sql)

        # 3. Executa agent SQL (só se pipeline teve sucesso)
        if pipe_result.success and agent_sql:
            agent_result = execute_sql(agent_sql)
            if agent_result.error:
                agent_error = agent_result.error
        else:
            agent_result = _empty_result
            if not agent_error:
                agent_error = "Pipeline falhou sem SQL gerado"

        # 4. Compara GT ⊆ agent
        passed = False
        if not gt_result.error and not agent_result.error and agent_sql:
            passed = is_subset(gt_result.rows, agent_result.rows)

        # 5. Determina status para o audit
        if gt_result.error or agent_result.error:
            status = "ERROR"
        elif passed:
            status = "PASS"
        else:
            status = "FAIL"

        # 6. Atualiza contadores e barra de progresso
        if status == "PASS":
            n_pass += 1
        elif status == "ERROR":
            n_error += 1
        else:
            n_fail += 1

        print(_render_progress(i, total, n_pass, n_fail, n_error), end="", flush=True)

        record = EvalRecord(
            id=entry.id,
            question=entry.question,
            difficulty=entry.difficulty,
            ground_truth_sql=entry.ground_truth_sql,
            agent_sql=agent_sql,
            passed=passed,
            gt_error=gt_result.error,
            agent_error=agent_error,
            gt_total_rows=gt_result.total_rows,
            agent_total_rows=agent_result.total_rows,
            latency_ms=latency_ms,
            repair_attempts=pipe_result.repair_attempts,
        )
        records.append(record)

        audit_entry = build_audit_entry(
            entry_id=entry.id,
            question=entry.question,
            gt_sql=entry.ground_truth_sql,
            agent_sql=agent_sql,
            gt_result=gt_result,
            agent_result=agent_result,
            status=status,
        )
        audit_entries.append(audit_entry)

    print()  # nova linha após a barra de progresso
    return records, audit_entries
