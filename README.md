# agent_datasus — Text-to-SQL sobre SIH-RD DATASUS

Pipeline Text-to-SQL que converte perguntas em linguagem natural em SQL DuckDB
e consulta diretamente o banco de dados do SIH-RD (Sistema de Informações
Hospitalares) do DATASUS — 18,4 milhões de internações, RS + MA, 2008–2023.

---

## Quickstart

### 1. Pré-requisitos

```bash
# Python 3.12 e venv já configurados
.venv/bin/pip install -r requirements.txt   # se existir, ou instale manualmente
```

Dependências principais: `duckdb`, `openai`, `fastapi`, `uvicorn`, `streamlit`,
`sqlglot`, `numpy`, `python-dotenv`, `pydantic>=2`.

### 2. Configuração

```bash
# .env (já configurado)
DATABASE_PATH=duckdb:////caminho/para/sihrd5.duckdb
OPENAI_API_KEY=sk-...
```

### 3. Validar conexão com o banco

```bash
python scripts/validate_connection.py
```

### 4. Construir índices de embeddings (rodar uma vez)

```bash
python scripts/build_indexes.py
```

Gera `.vector_store/schema_columns.json` e `.vector_store/few_shot_pairs.json`.
Custo: ~$0.001 e ~4 segundos. Roda novamente apenas se o schema ou os pares NL-SQL mudarem.

### 5. Iniciar a API

```bash
python scripts/serve.py
# → http://localhost:8000
# → Docs interativos: http://localhost:8000/docs
```

### 6. Iniciar a interface de chat

```bash
# Em outro terminal (API deve estar rodando, ou modo standalone):
.venv/bin/streamlit run scripts/chat.py
# → http://localhost:8501
```

---

## Uso via API

### `POST /query` — Executar uma pergunta

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Quais os 5 diagnósticos mais frequentes no Maranhão?"}'
```

```json
{
  "success": true,
  "question": "Quais os 5 diagnósticos mais frequentes no Maranhão?",
  "sql": "SELECT i.DIAG_PRINC AS cid, c.CD_DESCRICAO AS descricao, COUNT(*) AS total...",
  "columns": ["cid", "descricao", "total_internacoes"],
  "rows": [["O800", "Parto espontaneo cefalico", 521964], ...],
  "row_count": 5,
  "repair_attempts": 0,
  "latency_ms": 4200,
  "tokens_used": {"input": 1707, "output": 102, "total": 1809},
  "tables_used": ["internacoes", "cid", "municipios"],
  "error": null
}
```

### `GET /health` — Status do sistema

```bash
curl http://localhost:8000/health
```

### `GET /examples` — Exemplos de perguntas suportadas

```bash
curl http://localhost:8000/examples
```

### `GET /traces/summary` — Métricas de execução

```bash
curl http://localhost:8000/traces/summary
```

Documentação interativa completa (Swagger UI): `http://localhost:8000/docs`

---

## Perguntas de exemplo suportadas

### Simples
- Quantas internações ocorreram em 2022?
- Qual o custo total pago pelo SUS em internações?
- Quantos óbitos foram registrados no total?
- Qual o tempo médio de internação em dias?

### Médio
- Quais os 10 diagnósticos mais frequentes?
- Qual a taxa de mortalidade hospitalar por especialidade?
- Quantas internações de mulheres ocorreram em 2023?
- Quais os 5 hospitais com maior número de internações no MA?
- Qual a distribuição de internações por raça/cor?
- Qual a proporção de partos normais vs cesarianas por ano?

### Difícil (multi-join, CTE, séries temporais)
- Compare o perfil de internações entre RS e MA
- Como a mortalidade hospitalar evoluiu durante a pandemia?
- Qual a relação entre IDH municipal e taxa de mortalidade hospitalar?
- Compare a mortalidade entre municípios de alto e baixo IDH no RS
- Existe diferença na mortalidade entre pacientes brancos e pardos em internações clínicas?
- Qual a escolaridade mais frequente entre pacientes internados com AVC?

---

## Arquitetura

```
Pergunta (NL)
    ↓
Schema Linking (embeddings)      → seleciona tabelas relevantes
    ↓
Few-Shot Selection (embeddings)  → seleciona exemplos similares
    ↓
LLM (gpt-4o-mini, T=0)          → gera SQL
    ↓
Validação Sintática (sqlglot)   → verifica sem conexão
    ↓
Repair Loop (máx 3x)            → LLM com erro anterior no contexto
    ↓
Execução Segura (DuckDB RO)     → read-only, LIMIT 500, blocklist DDL
    ↓
Resultado + Trace Log
```

**Desempenho médio (138 execuções históricas):**
- Latência: ~3.0s por query
- Tokens: ~1.533 (64% menos que schema completo)
- Taxa de sucesso: 100%
- Repairs: 0

---

## Scripts disponíveis

| Script | Descrição |
|--------|-----------|
| `python scripts/validate_connection.py` | Valida conexão e inspeciona o banco |
| `python scripts/build_indexes.py` | Constrói índices de embeddings (rodar 1x) |
| `python scripts/build_indexes.py --force` | Re-embedda tudo do zero |
| `python scripts/serve.py` | Inicia API em porta 8000 |
| `python scripts/serve.py --port 8080` | Porta customizada |
| `python scripts/serve.py --reload` | Modo dev com hot reload |
| `streamlit run scripts/chat.py` | Interface de chat |
| `python scripts/demo_pipeline.py` | Demo com 4 perguntas padrão |
| `python scripts/demo_pipeline.py "pergunta"` | Pergunta customizada |
| `python scripts/evaluate.py` | Avaliação EX nos 30 pares anotados |
| `python scripts/evaluate.py --category difícil` | Avaliação por categoria |

---

## Limitações conhecidas

| Limitação | Detalhe |
|-----------|---------|
| **Escopo geográfico** | Banco contém apenas RS e MA. Perguntas sobre outros estados retornarão 0 linhas. |
| **Período** | Dados de 2008 a 2023 apenas. |
| **Natureza dos hospitais** | Tabela `hospital` não tem dimensão de decodificação para `NATUREZA`/`NAT_JUR`. |
| **Encoding** | Nomes de municípios têm issue de encoding (latin1/UTF-8) para alguns caracteres especiais. |
| **Latência** | ~3s por query (tempo do LLM). Queries ao DuckDB em si são <1s. |
| **Max rows** | Resultado limitado a 500 linhas por padrão. |
| **Read-only** | A API nunca modifica o banco — somente SELECT. |

---

## Estrutura do projeto

```
agent_datasus/
├── .env                        ← configurações (DATABASE_PATH, OPENAI_API_KEY)
├── .vector_store/              ← índices de embeddings persistidos
├── logs/traces.jsonl           ← histórico de execuções
├── reports/                    ← relatórios de avaliação
├── src/
│   ├── db/
│   │   ├── connection.py       ← DatabaseConnection + get_connection()
│   │   └── schema.py           ← get_schema_info() + build_schema_prompt()
│   ├── text2sql/
│   │   ├── domain_dict.py      ← dicionário NL→schema (40 entradas)
│   │   ├── nl_sql_pairs.py     ← 30 pares NL-SQL anotados
│   │   ├── vector_store.py     ← EmbeddingStore (OpenAI + numpy)
│   │   ├── schema_linker.py    ← schema linking semântico
│   │   ├── few_shot_store.py   ← few-shot selection semântico
│   │   ├── logger.py           ← TraceLogger (JSON Lines)
│   │   └── pipeline.py         ← Text2SQLPipeline (pipeline completo)
│   └── api/
│       ├── models.py           ← Pydantic v2 request/response
│       ├── startup.py          ← lifespan + AppState singleton
│       └── app.py              ← FastAPI app + endpoints
└── scripts/
    ├── validate_connection.py
    ├── build_indexes.py
    ├── serve.py                ← uvicorn launcher
    ├── chat.py                 ← Streamlit interface
    ├── demo_pipeline.py
    └── evaluate.py
```
