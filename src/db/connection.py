"""
Camada de conexão reutilizável com DuckDB via .env
"""
from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
from dotenv import load_dotenv

# Carrega .env a partir da raiz do projeto (dois níveis acima deste arquivo)
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)

def _parse_database_path() -> Path:
    raw = os.getenv("DATABASE_PATH", "")
    if not raw:
        raise EnvironmentError("DATABASE_PATH não definido no .env")

    # Suporta: duckdb:////abs/path, duckdb:///abs/path, duckdb://rel/path, /abs/path
    # duckdb:////home → strip "duckdb://" → "//home" → Path("//home") == "/home" (UNC-like, mas no Linux é igual)
    if raw.startswith("duckdb://"):
        rest = raw[len("duckdb://"):]
        # Normaliza múltiplas barras iniciais mantendo path absoluto
        path = Path("/" + rest.lstrip("/"))
    else:
        path = Path(raw)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {path}")

    return path


class DatabaseConnection:
    """
    Wrapper thread-safe sobre duckdb.DuckDBPyConnection.

    Uso como context manager (recomendado):
        with DatabaseConnection() as conn:
            rows = conn.execute("SELECT 1").fetchall()

    Ou instanciação direta (para reuso em pipelines):
        db = DatabaseConnection(read_only=True)
        db.connect()
        ...
        db.close()
    """

    _local = threading.local()

    def __init__(self, read_only: bool = True) -> None:
        self._path = _parse_database_path()
        self._read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> "DatabaseConnection":
        if self._conn is None:
            self._conn = duckdb.connect(str(self._path), read_only=self._read_only)
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self.connect()
        return self._conn  # type: ignore[return-value]

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Helpers utilitários
    # ------------------------------------------------------------------

    def execute(self, query: str, params: list | None = None):
        """Executa query e retorna resultado bruto do DuckDB."""
        if self._conn is None:
            raise RuntimeError("Conexão não iniciada. Use .connect() ou o context manager.")
        return self._conn.execute(query, params or [])

    def fetchall(self, query: str, params: list | None = None) -> list[tuple]:
        return self.execute(query, params).fetchall()

    def fetchone(self, query: str, params: list | None = None) -> tuple | None:
        return self.execute(query, params).fetchone()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def is_connected(self) -> bool:
        return self._conn is not None


# ------------------------------------------------------------------
# Factory global (singleton por thread, read-only)
# ------------------------------------------------------------------

@contextmanager
def get_connection(read_only: bool = True) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager de alto nível para obter conexão segura.

    Exemplo:
        with get_connection() as conn:
            result = conn.execute("SELECT COUNT(*) FROM internacoes").fetchone()
    """
    db = DatabaseConnection(read_only=read_only)
    try:
        yield db.__enter__()
    finally:
        db.close()
