"""
Modelos Pydantic v2 para request/response da API.
"""
from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


# ── Request ──────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=500,
        description="Pergunta em linguagem natural sobre o banco SIH-RD DATASUS.",
        examples=["Quantas internações ocorreram em 2022?"],
    )


# ── Response ─────────────────────────────────────────────────────────────────

class QueryResponse(BaseModel):
    success: bool
    question: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    repair_attempts: int
    latency_ms: int
    tokens_used: dict[str, int]
    tables_used: list[str]
    schema_tables_selected: list[str]
    error: str | None = None


class HealthResponse(BaseModel):
    status: str                        # "ok" | "degraded" | "error"
    database_connected: bool
    indexes_loaded: bool
    schema_index_docs: int
    few_shot_index_docs: int
    total_traces: int
    success_rate_pct: float


class ExamplesResponse(BaseModel):
    examples: list[ExampleItem]


class ExampleItem(BaseModel):
    question: str
    difficulty: str
    tags: list[str]


class TracesSummaryResponse(BaseModel):
    total: int
    success_rate: float
    failures: int
    with_repairs: int
    avg_latency_ms: int
    avg_tokens_total: int
    total_tokens: int
    most_used_tables: list[list[Any]]


class SubResult(BaseModel):
    question: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    success: bool
    error: str | None = None


class AgentQueryResponse(BaseModel):
    success: bool
    question: str
    query_type: str              # "simple" | "complex"
    complexity_reason: str
    sql: str                     # SQL final (vazio se complex)
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    answer: str                  # resposta em linguagem natural
    semantic_warnings: list[str]
    repair_attempts: int
    latency_ms: int
    tokens_used: dict[str, int]
    tables_used: list[str]
    schema_tables_selected: list[str]
    sub_questions: list[str]     # apenas para query_type="complex"
    sub_results: list[SubResult] # apenas para query_type="complex"
    error: str | None = None
