"""
API REST — Text-to-SQL sobre SIH-RD DATASUS

Endpoints:
  POST /query              → pipeline Fase 2 (SQL + resultado)
  POST /agent/query        → agente multiagente Fase 4 (classificação + SQL + resposta NL)
  GET  /health             → status do banco e pipeline
  GET  /examples           → lista de perguntas de exemplo
  GET  /traces/summary     → métricas agregadas de todas as execuções

Design:
  - Endpoints síncronos (DuckDB não é async-native) rodando via threadpool do FastAPI
  - Pipeline singleton carregado no lifespan (não re-instanciado por request)
  - Timeout de 60s por request via asyncio
  - Erros de negócio retornam 200 com success=False (não 500)
  - Erros de infraestrutura retornam 503
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.agent.graph import run_agent
from src.api.models import (
    AgentQueryResponse,
    ExampleItem,
    ExamplesResponse,
    HealthResponse,
    QueryRequest,
    QueryResponse,
    SubResult,
    TracesSummaryResponse,
)
from src.api.startup import get_state, lifespan
from src.db.connection import get_connection
from src.text2sql.nl_sql_pairs import NL_SQL_PAIRS

REQUEST_TIMEOUT_SECONDS = 60
_executor = ThreadPoolExecutor(max_workers=4)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="DATASUS Text-to-SQL API",
    description=(
        "Pipeline Text-to-SQL sobre o banco SIH-RD DATASUS. "
        "Converte perguntas em linguagem natural em SQL DuckDB "
        "e retorna os resultados diretamente do banco de 7,1 GB."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post(
    "/query",
    response_model=QueryResponse,
    summary="Executa uma pergunta em linguagem natural",
    description=(
        "Converte a pergunta em SQL via LLM (gpt-4o-mini), valida, executa no DuckDB "
        "e retorna o resultado junto com o SQL gerado e métricas da execução."
    ),
)
async def query(body: QueryRequest) -> QueryResponse:
    state = get_state()

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(_executor, state.pipeline.run, body.question),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"Query excedeu o timeout de {REQUEST_TIMEOUT_SECONDS}s.",
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Erro interno no pipeline: {e}")

    return QueryResponse(
        success=result.success,
        question=result.question,
        sql=result.sql,
        columns=result.columns,
        rows=[list(row) for row in result.rows],
        row_count=len(result.rows),
        repair_attempts=result.repair_attempts,
        latency_ms=result.latency_ms,
        tokens_used=result.tokens_used,
        tables_used=result.tables_used,
        schema_tables_selected=result.schema_tables_selected,
        error=result.error,
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Status do sistema",
)
async def health() -> HealthResponse:
    state = get_state()

    # Testa conexão com o banco
    db_ok = False
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1").fetchone()
        db_ok = True
    except Exception:
        pass

    summary = state.logger.summary()
    indexes_ok = state.schema_store.is_built and state.few_shot_store.is_built

    return HealthResponse(
        status="ok" if db_ok and indexes_ok else "degraded",
        database_connected=db_ok,
        indexes_loaded=indexes_ok,
        schema_index_docs=state.schema_store.size,
        few_shot_index_docs=state.few_shot_store.size,
        total_traces=summary.get("total", 0),
        success_rate_pct=summary.get("success_rate", 0.0),
    )


@app.get(
    "/examples",
    response_model=ExamplesResponse,
    summary="Lista exemplos de perguntas suportadas",
)
async def examples() -> ExamplesResponse:
    items = [
        ExampleItem(
            question=p.question,
            difficulty=p.difficulty,
            tags=p.tags,
        )
        for p in NL_SQL_PAIRS
    ]
    return ExamplesResponse(examples=items)


@app.get(
    "/traces/summary",
    response_model=TracesSummaryResponse,
    summary="Resumo agregado de todas as execuções",
)
async def traces_summary() -> TracesSummaryResponse:
    state = get_state()
    s = state.logger.summary()

    if s.get("total", 0) == 0:
        return TracesSummaryResponse(
            total=0, success_rate=0, failures=0, with_repairs=0,
            avg_latency_ms=0, avg_tokens_total=0, total_tokens=0,
            most_used_tables=[],
        )

    return TracesSummaryResponse(
        total=s["total"],
        success_rate=s["success_rate"],
        failures=s["failures"],
        with_repairs=s["with_repairs"],
        avg_latency_ms=s["avg_latency_ms"],
        avg_tokens_total=s["avg_tokens_total"],
        total_tokens=s["total_tokens"],
        most_used_tables=[list(t) for t in s["most_used_tables"]],
    )


# ---------------------------------------------------------------------------
# Endpoint do agente multiagente (Fase 4)
# ---------------------------------------------------------------------------

@app.post(
    "/agent/query",
    response_model=AgentQueryResponse,
    summary="Executa pergunta via agente multiagente (Fase 4)",
    description=(
        "Classifica a query (simples/complexa), aplica decomposição se necessário, "
        "valida o resultado semanticamente e retorna uma resposta em linguagem natural "
        "além do SQL e dados brutos."
    ),
)
async def agent_query(body: QueryRequest) -> AgentQueryResponse:
    state = get_state()

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                _executor,
                lambda: run_agent(body.question, compiled_graph=state.agent_graph),
            ),
            timeout=REQUEST_TIMEOUT_SECONDS * 2,  # agente pode ser mais lento (multi-LLM)
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Agente excedeu o timeout.")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Erro interno no agente: {e}")

    return AgentQueryResponse(
        success=result.success,
        question=result.question,
        query_type=result.query_type,
        complexity_reason=result.complexity_reason,
        sql=result.sql,
        columns=result.columns,
        rows=result.rows,
        row_count=len(result.rows),
        answer=result.answer,
        semantic_warnings=result.semantic_warnings,
        repair_attempts=result.repair_attempts,
        latency_ms=result.latency_ms,
        tokens_used=result.total_tokens,
        tables_used=result.tables_used,
        schema_tables_selected=result.schema_tables,
        sub_questions=result.sub_questions,
        sub_results=[SubResult(**sr) for sr in result.sub_results],
        error=result.error,
    )


# ---------------------------------------------------------------------------
# Handler global de erros não tratados
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"detail": f"Erro inesperado: {type(exc).__name__}: {exc}"},
    )
