"""
Vector store leve baseado em OpenAI embeddings + numpy cosine similarity.

Persiste embeddings em disco (JSON) para evitar re-embedding a cada execução.
Sem dependências externas além de numpy e openai (já presentes).

Design:
  - Uma instância de EmbeddingStore por coleção (schema, few_shot, etc.)
  - Embedding: text-embedding-3-small (1536 dims, custo mínimo)
  - Persistência: .vector_store/<collection>.json
  - Busca: cosine similarity em numpy — rápido para coleções < 10k docs
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)

EMBEDDING_MODEL = "text-embedding-3-small"
STORE_DIR = Path(__file__).resolve().parents[2] / ".vector_store"
STORE_DIR.mkdir(exist_ok=True)


class EmbeddingStore:
    """
    Vector store persistente para uma coleção de documentos.

    Uso:
        store = EmbeddingStore("schema_columns")
        store.add("internacoes.MORTE", "MORTE é BOOLEAN, óbito hospitalar", {"table": "internacoes"})
        store.build()  # gera embeddings e persiste
        results = store.search("taxa de mortalidade", top_k=5)
    """

    def __init__(self, collection: str) -> None:
        self._collection = collection
        self._path = STORE_DIR / f"{collection}.json"
        self._client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Estrutura interna: lista de {id, text, metadata, embedding}
        self._docs: list[dict[str, Any]] = []
        self._embeddings: np.ndarray | None = None  # shape (n_docs, dim)

        if self._path.exists():
            self._load()

    # ── Gerenciamento de documentos ──────────────────────────────────────────

    def add(self, doc_id: str, text: str, metadata: dict | None = None) -> None:
        """Adiciona um documento (sem gerar embedding ainda)."""
        self._docs.append({
            "id": doc_id,
            "text": text,
            "metadata": metadata or {},
            "embedding": None,
            "text_hash": _hash(text),
        })
        self._embeddings = None  # invalida cache

    def clear(self) -> None:
        self._docs = []
        self._embeddings = None
        if self._path.exists():
            self._path.unlink()

    # ── Geração de embeddings ────────────────────────────────────────────────

    def build(self, force: bool = False) -> int:
        """
        Gera embeddings para todos os docs sem embedding (ou todos se force=True).
        Persiste em disco. Retorna número de embeddings gerados.
        """
        pending = [d for d in self._docs if d["embedding"] is None or force]
        if not pending:
            self._rebuild_matrix()
            return 0

        texts = [d["text"] for d in pending]
        embeddings = self._embed_batch(texts)

        pending_iter = iter(embeddings)
        for doc in self._docs:
            if doc["embedding"] is None or force:
                doc["embedding"] = next(pending_iter)

        self._rebuild_matrix()
        self._save()
        return len(pending)

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Chama OpenAI em batches de 100 (limite da API)."""
        all_embeddings: list[list[float]] = []
        batch_size = 100
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            resp = self._client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
            all_embeddings.extend([item.embedding for item in resp.data])
        return all_embeddings

    def _rebuild_matrix(self) -> None:
        """Reconstrói a matriz numpy a partir dos docs com embedding."""
        embedded = [d for d in self._docs if d["embedding"] is not None]
        if not embedded:
            self._embeddings = None
            return
        self._embeddings = np.array([d["embedding"] for d in embedded], dtype=np.float32)

    # ── Busca ────────────────────────────────────────────────────────────────

    def search(self, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        """
        Retorna os top_k documentos mais similares à query.
        Cada resultado: {"id", "text", "metadata", "score"}
        """
        if self._embeddings is None or len(self._embeddings) == 0:
            raise RuntimeError(f"Índice '{self._collection}' vazio. Execute build() primeiro.")

        q_emb = np.array(self._embed_batch([query])[0], dtype=np.float32)
        scores = _cosine_similarity_batch(q_emb, self._embeddings)
        top_indices = np.argsort(scores)[::-1][:top_k]

        embedded_docs = [d for d in self._docs if d["embedding"] is not None]
        return [
            {
                "id": embedded_docs[i]["id"],
                "text": embedded_docs[i]["text"],
                "metadata": embedded_docs[i]["metadata"],
                "score": float(scores[i]),
            }
            for i in top_indices
        ]

    # ── Persistência ─────────────────────────────────────────────────────────

    def _save(self) -> None:
        data = {"collection": self._collection, "docs": self._docs}
        self._path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    def _load(self) -> None:
        data = json.loads(self._path.read_text(encoding="utf-8"))
        self._docs = data.get("docs", [])
        self._rebuild_matrix()

    @property
    def size(self) -> int:
        return len(self._docs)

    @property
    def is_built(self) -> bool:
        return self._embeddings is not None and len(self._embeddings) > 0


# ── Helpers ──────────────────────────────────────────────────────────────────

def _cosine_similarity_batch(query: np.ndarray, matrix: np.ndarray) -> np.ndarray:
    """Cosine similarity entre um vetor e uma matriz de vetores."""
    query_norm = query / (np.linalg.norm(query) + 1e-10)
    matrix_norms = matrix / (np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-10)
    return matrix_norms @ query_norm


def _hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:8]
