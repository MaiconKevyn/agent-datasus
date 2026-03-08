"""
Gerenciamento de ciclo de vida da API.

Carrega índices de embeddings e o pipeline uma única vez no startup,
compartilhando via AppState para todos os requests sem re-instanciar.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from src.agent.graph import build_graph
from src.text2sql.few_shot_store import build_few_shot_index
from src.text2sql.logger import TraceLogger
from src.text2sql.pipeline import Text2SQLPipeline
from src.text2sql.schema_linker import build_schema_index
from src.text2sql.vector_store import EmbeddingStore


@dataclass
class AppState:
    pipeline: Text2SQLPipeline
    agent_graph: object          # CompiledStateGraph
    schema_store: EmbeddingStore
    few_shot_store: EmbeddingStore
    logger: TraceLogger


_state: AppState | None = None


def get_state() -> AppState:
    if _state is None:
        raise RuntimeError("AppState não inicializado. O lifespan não foi executado.")
    return _state


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Executa no startup e shutdown da aplicação.
    Garante que índices e pipeline estejam prontos antes do primeiro request.
    """
    global _state

    print("[startup] Carregando índices de embeddings...")
    schema_store = build_schema_index()
    few_shot_store = build_few_shot_index()

    if not schema_store.is_built or not few_shot_store.is_built:
        print("[startup] AVISO: índices não encontrados. Execute: python scripts/build_indexes.py")

    print(f"[startup] Schema index: {schema_store.size} docs")
    print(f"[startup] Few-shot index: {few_shot_store.size} docs")

    pipeline = Text2SQLPipeline(
        schema_store=schema_store,
        few_shot_store=few_shot_store,
    )
    agent_graph = build_graph(schema_store, few_shot_store)
    logger = TraceLogger()

    _state = AppState(
        pipeline=pipeline,
        agent_graph=agent_graph,
        schema_store=schema_store,
        few_shot_store=few_shot_store,
        logger=logger,
    )

    print("[startup] Pipeline e grafo multiagente prontos. API disponível.")
    yield

    # Shutdown: nada a limpar (DuckDB é read-only, conexões são por request)
    print("[shutdown] Encerrando.")
