#!/usr/bin/env python3
"""
Inicia a API REST do Text-to-SQL.

Uso:
    python scripts/serve.py             # porta padrão 8000
    python scripts/serve.py --port 8080
    python scripts/serve.py --reload    # modo dev com hot reload

Acesse:
    Docs interativos: http://localhost:8000/docs
    Health check:     http://localhost:8000/health
    Exemplos:         http://localhost:8000/examples
"""
import sys
from pathlib import Path

# Garante que o root do projeto está no path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn

PORT = 8000
for i, arg in enumerate(sys.argv[1:]):
    if arg == "--port" and i + 2 <= len(sys.argv) - 1:
        PORT = int(sys.argv[i + 2])

reload = "--reload" in sys.argv

if __name__ == "__main__":
    uvicorn.run(
        "src.api.app:app",
        host="0.0.0.0",
        port=PORT,
        reload=reload,
        log_level="info",
    )
