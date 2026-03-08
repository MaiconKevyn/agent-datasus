"""
Logger de traces estruturado para o pipeline Text-to-SQL.

Persiste cada execução em logs/traces.jsonl (JSON Lines).
Design compatível com a interface do Langfuse para migração futura:
  - Cada trace tem: trace_id, timestamp, spans por etapa, metadados
  - Permite análise offline de acurácia, latência e custo

Uso:
    logger = TraceLogger()
    with logger.trace("Quantas internações em 2022?") as t:
        t.log_span("schema_linking", tables=["internacoes"])
        t.log_span("llm_call", tokens=2085, latency_ms=3200)
        t.set_result(success=True, sql="SELECT ...", rows=1)
"""
from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

LOGS_DIR = Path(__file__).resolve().parents[2] / "logs"
LOGS_DIR.mkdir(exist_ok=True)
TRACES_FILE = LOGS_DIR / "traces.jsonl"


@dataclass
class Span:
    name: str
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: _now())


@dataclass
class Trace:
    trace_id: str
    question: str
    started_at: str
    spans: list[Span] = field(default_factory=list)
    # Preenchidos ao finalizar
    sql: str = ""
    success: bool = False
    error: str | None = None
    rows_returned: int = 0
    repair_attempts: int = 0
    latency_ms: int = 0
    tokens_input: int = 0
    tokens_output: int = 0
    tokens_total: int = 0
    tables_used: list[str] = field(default_factory=list)
    schema_tables_selected: list[str] = field(default_factory=list)

    def log_span(self, name: str, **kwargs) -> None:
        self.spans.append(Span(name=name, data=kwargs))

    def set_result(
        self,
        *,
        success: bool,
        sql: str = "",
        error: str | None = None,
        rows_returned: int = 0,
        repair_attempts: int = 0,
        latency_ms: int = 0,
        tokens: dict[str, int] | None = None,
        tables_used: list[str] | None = None,
        schema_tables_selected: list[str] | None = None,
    ) -> None:
        self.success = success
        self.sql = sql
        self.error = error
        self.rows_returned = rows_returned
        self.repair_attempts = repair_attempts
        self.latency_ms = latency_ms
        if tokens:
            self.tokens_input = tokens.get("input", 0)
            self.tokens_output = tokens.get("output", 0)
            self.tokens_total = tokens.get("total", 0)
        self.tables_used = tables_used or []
        self.schema_tables_selected = schema_tables_selected or []


class TraceLogger:
    """Logger de traces JSON Lines — um trace por linha."""

    def __init__(self, output_file: Path = TRACES_FILE) -> None:
        self._file = output_file

    @contextmanager
    def trace(self, question: str) -> Generator[Trace, None, None]:
        """
        Context manager que cria um trace, cede ao caller e persiste ao sair.

        Uso:
            with logger.trace(question) as t:
                t.log_span("schema_linking", tables=[...])
                t.set_result(success=True, ...)
        """
        t = Trace(
            trace_id=str(uuid.uuid4()),
            question=question,
            started_at=_now(),
        )
        try:
            yield t
        finally:
            self._write(t)

    def _write(self, trace: Trace) -> None:
        record = {
            "trace_id": trace.trace_id,
            "timestamp": trace.started_at,
            "question": trace.question,
            "success": trace.success,
            "sql": trace.sql,
            "error": trace.error,
            "rows_returned": trace.rows_returned,
            "repair_attempts": trace.repair_attempts,
            "latency_ms": trace.latency_ms,
            "tokens": {
                "input": trace.tokens_input,
                "output": trace.tokens_output,
                "total": trace.tokens_total,
            },
            "tables_used": trace.tables_used,
            "schema_tables_selected": trace.schema_tables_selected,
            "spans": [asdict(s) for s in trace.spans],
        }
        with self._file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # ── Análise de traces ─────────────────────────────────────────────────────

    def load_traces(self) -> list[dict]:
        """Carrega todos os traces do arquivo."""
        if not self._file.exists():
            return []
        traces = []
        for line in self._file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                traces.append(json.loads(line))
        return traces

    def summary(self) -> dict[str, Any]:
        """Resumo agregado de todos os traces."""
        traces = self.load_traces()
        if not traces:
            return {"total": 0}

        successful = [t for t in traces if t["success"]]
        failed = [t for t in traces if not t["success"]]
        repaired = [t for t in traces if t["repair_attempts"] > 0]

        return {
            "total": len(traces),
            "success_rate": round(len(successful) / len(traces) * 100, 1),
            "failures": len(failed),
            "with_repairs": len(repaired),
            "avg_latency_ms": round(sum(t["latency_ms"] for t in traces) / len(traces)),
            "avg_tokens_total": round(sum(t["tokens"]["total"] for t in traces) / len(traces)),
            "total_tokens": sum(t["tokens"]["total"] for t in traces),
            "most_used_tables": _top_items(
                [tbl for t in traces for tbl in t["tables_used"]], n=5
            ),
        }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _top_items(items: list[str], n: int) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item] = counts.get(item, 0) + 1
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:n]
