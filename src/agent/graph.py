"""
Grafo LangGraph — Text-to-SQL Multiagente (Fase 4)

Topologia:
                         ┌──────────────────────────────────────────┐
                         │               [repair loop]               │
                         │  repair ←── validate_syntax (erro)        │
  START                  │  repair ←── execute (erro)                │
    │                    │  repair ←── validate_result (erro)        │
    ▼                    │                                            │
  classify               │  repair → generate_sql → ...              │
    │                    └──────────────────────────────────────────┘
    ├─ "simple" ──► schema_link ──► generate_sql ──► validate_syntax
    │                                                      │ ok
    │                                                   execute
    │                                                      │ ok
    │                                               validate_result
    │                                                      │ ok
    └─ "complex" ──► decompose ──────────────────────► explain ──► END
                                                      ▲
                                                      │ ok (também do path simples)
                                               validate_result

Nó especial "end_error":
  Nó terminal de falha — só ativado quando tentativas se esgotam.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from langgraph.graph import END, START, StateGraph

from src.agent.nodes import (
    classify_node,
    execute_node,
    explain_node,
    generate_sql_node,
    make_decompose_node,
    make_schema_link_node,
    repair_node,
    validate_result_node,
    validate_syntax_node,
)
from src.agent.routing import (
    route_after_classify,
    route_after_execute,
    route_after_repair,
    route_after_validate_result,
    route_after_validate_syntax,
)
from src.agent.state import AgentState
from src.text2sql.logger import TraceLogger


# ---------------------------------------------------------------------------
# Nó terminal de erro
# ---------------------------------------------------------------------------

def end_error_node(state: AgentState) -> dict:
    """Nó terminal ativado quando todas as tentativas de repair se esgotam."""
    return {
        "success": False,
        "answer": "",
        "error": state.get("last_error") or "Número máximo de tentativas atingido.",
    }


# ---------------------------------------------------------------------------
# Construção do grafo
# ---------------------------------------------------------------------------

def build_graph(schema_store=None, few_shot_store=None) -> StateGraph:
    """
    Monta e compila o grafo LangGraph.

    Args:
        schema_store: EmbeddingStore de colunas de schema (Fase 2).
        few_shot_store: EmbeddingStore de pares NL-SQL (Fase 2).

    Returns:
        Grafo compilado (CompiledStateGraph) pronto para invocar.
    """
    # Nós que dependem de stores são criados via factory (closure)
    schema_link_node = make_schema_link_node(schema_store, few_shot_store)
    decompose_node = make_decompose_node(schema_store, few_shot_store)

    graph = StateGraph(AgentState)

    # ── Registro de nós ──────────────────────────────────────────────────────
    graph.add_node("classify", classify_node)
    graph.add_node("schema_link", schema_link_node)
    graph.add_node("generate_sql", generate_sql_node)
    graph.add_node("validate_syntax", validate_syntax_node)
    graph.add_node("execute", execute_node)
    graph.add_node("validate_result", validate_result_node)
    graph.add_node("repair", repair_node)
    graph.add_node("decompose", decompose_node)
    graph.add_node("explain", explain_node)
    graph.add_node("end_error", end_error_node)

    # ── Arestas fixas ────────────────────────────────────────────────────────
    graph.add_edge(START, "classify")
    graph.add_edge("schema_link", "generate_sql")
    graph.add_edge("decompose", "explain")
    graph.add_edge("explain", END)
    graph.add_edge("end_error", END)

    # ── Arestas condicionais ─────────────────────────────────────────────────
    graph.add_conditional_edges(
        "classify",
        route_after_classify,
        {"schema_link": "schema_link", "decompose": "decompose"},
    )
    graph.add_conditional_edges(
        "validate_syntax",
        route_after_validate_syntax,
        {"execute": "execute", "repair": "repair", "end_error": "end_error"},
    )
    graph.add_conditional_edges(
        "execute",
        route_after_execute,
        {"validate_result": "validate_result", "repair": "repair", "end_error": "end_error"},
    )
    graph.add_conditional_edges(
        "validate_result",
        route_after_validate_result,
        {"explain": "explain", "repair": "repair"},
    )
    graph.add_conditional_edges(
        "repair",
        route_after_repair,
        {"generate_sql": "generate_sql"},
    )

    # ── Arestas de generate_sql ───────────────────────────────────────────────
    # generate_sql sempre avança para validate_syntax
    graph.add_edge("generate_sql", "validate_syntax")

    return graph.compile()


# ---------------------------------------------------------------------------
# Resultado estruturado
# ---------------------------------------------------------------------------

@dataclass
class AgentResult:
    question: str
    query_type: str
    complexity_reason: str
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    answer: str
    success: bool
    error: str | None
    semantic_warnings: list[str]
    repair_attempts: int
    total_tokens: dict[str, int]
    latency_ms: int
    tables_used: list[str]
    schema_tables: list[str]
    # Apenas para queries complexas
    sub_questions: list[str]
    sub_results: list[dict]


# ---------------------------------------------------------------------------
# Função de execução com logging
# ---------------------------------------------------------------------------

_logger = TraceLogger()


def run_agent(
    question: str,
    compiled_graph=None,
    schema_store=None,
    few_shot_store=None,
) -> AgentResult:
    """
    Executa o agente sobre uma pergunta e retorna AgentResult.

    Se compiled_graph for None, constrói o grafo automaticamente.
    """
    if compiled_graph is None:
        compiled_graph = build_graph(schema_store, few_shot_store)

    t0 = time.monotonic()

    initial_state: AgentState = {
        "question": question,
        "sql_attempts": 0,
        "repair_attempts": 0,
        "semantic_warnings": [],
        "total_tokens": {"input": 0, "output": 0, "total": 0},
    }

    final_state: AgentState = compiled_graph.invoke(initial_state)

    latency_ms = int((time.monotonic() - t0) * 1000)

    result = AgentResult(
        question=question,
        query_type=final_state.get("query_type", "simple"),
        complexity_reason=final_state.get("complexity_reason", ""),
        sql=final_state.get("sql", ""),
        columns=final_state.get("columns", []),
        rows=final_state.get("rows", []),
        answer=final_state.get("answer", ""),
        success=final_state.get("success", False),
        error=final_state.get("error"),
        semantic_warnings=final_state.get("semantic_warnings", []),
        repair_attempts=final_state.get("repair_attempts", 0),
        total_tokens=final_state.get("total_tokens", {}),
        latency_ms=latency_ms,
        tables_used=final_state.get("tables_used", []),
        schema_tables=final_state.get("schema_tables", []),
        sub_questions=final_state.get("sub_questions", []),
        sub_results=final_state.get("sub_results", []),
    )

    # Persiste trace
    with _logger.trace(question) as t:
        t.log_span("agent_graph", query_type=result.query_type, reason=result.complexity_reason)
        t.set_result(
            success=result.success,
            sql=result.sql,
            error=result.error,
            rows_returned=len(result.rows),
            repair_attempts=result.repair_attempts,
            latency_ms=latency_ms,
            tokens=result.total_tokens,
            tables_used=result.tables_used,
            schema_tables_selected=result.schema_tables,
        )

    return result
