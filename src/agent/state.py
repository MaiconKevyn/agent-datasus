"""
AgentState — estado compartilhado entre todos os nós do grafo LangGraph.

Cada nó recebe o state completo e retorna um dicionário com apenas os campos
que alterou (merge automático pelo LangGraph via reducer).

Design:
  - TypedDict obrigatório para LangGraph
  - Campos com None como default são opcionais (nem todo path os preenche)
  - total_tokens acumula ao longo de todas as chamadas LLM do grafo
"""
from __future__ import annotations

from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    # ── Input ────────────────────────────────────────────────────────────────
    question: str

    # ── Classificação ────────────────────────────────────────────────────────
    query_type: str           # "simple" | "complex"
    complexity_reason: str    # explicação da classificação

    # ── Schema linking ───────────────────────────────────────────────────────
    schema_prompt: str
    schema_tables: list[str]
    domain_rules: str
    few_shot: str

    # ── Geração e reparo de SQL ──────────────────────────────────────────────
    sql: str
    sql_attempts: int         # contador de tentativas (0-indexed)
    last_error: str | None    # erro da última tentativa (para repair prompt)

    # ── Execução ─────────────────────────────────────────────────────────────
    rows: list[list[Any]]
    columns: list[str]
    tables_used: list[str]

    # ── Validação semântica ──────────────────────────────────────────────────
    semantic_warnings: list[str]

    # ── Decomposição (path complexo) ─────────────────────────────────────────
    sub_questions: list[str]
    sub_results: list[dict[str, Any]]   # [{question, sql, rows, columns}, ...]

    # ── Resposta final ───────────────────────────────────────────────────────
    answer: str               # resposta em linguagem natural
    success: bool
    error: str | None

    # ── Rastreabilidade ──────────────────────────────────────────────────────
    repair_attempts: int
    total_tokens: dict[str, int]
    latency_ms: int


MAX_REPAIR_ATTEMPTS = 3
MAX_ROWS = 500
MODEL = "gpt-4o-mini"
