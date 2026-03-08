# agent_datasus — Diário Técnico do Projeto

Sistema Text-to-SQL sobre o banco SIH-RD DATASUS.
Domínio: internações hospitalares SUS | Estados: RS + MA | Período: 2008–2023 | 7,1 GB DuckDB.

---

## Como usar este arquivo

- Cada fase tem checklist de entregas e seção de detalhes técnicos.
- Ao validar uma entrega, marque com `[x]`.
- Ao iniciar uma fase, registre a data e decisões tomadas.
- Ao encerrar uma fase, registre achados, resultados de testes e lições.

---

## Visão Geral das Fases

| Fase | Objetivo | Status |
|------|----------|--------|
| 1 | Fundação: conexão, schema, pipeline básico | ✅ Concluída |
| 2 | Qualidade: embeddings, avaliação automática, observability | ✅ Concluída |
| 3 | Produto: API REST + interface de chat | ✅ Concluída |
| 4 | Multiagente com LangGraph | ✅ Concluída |

---

## Fase 1 — Fundação ✅

**Objetivo:** Estabelecer a base do sistema: conexão segura com o banco, extração de schema, pipeline Text-to-SQL funcional com repair loop.

**Status:** Concluída e validada em 07/03/2026.

### Checklist

- [x] Leitura e análise do `.env` com configuração do banco
- [x] Camada de conexão reutilizável (`src/db/connection.py`)
- [x] Extração de schema com anotação semântica (`src/db/schema.py`)
- [x] Script de validação da conexão (`scripts/validate_connection.py`)
- [x] Investigação profunda do banco (schema, FKs, distribuições, domínio)
- [x] Dicionário de termos do domínio NL→schema (`src/text2sql/domain_dict.py`)
- [x] 30 pares NL-SQL anotados (`src/text2sql/nl_sql_pairs.py`)
- [x] Pipeline Text-to-SQL end-to-end (`src/text2sql/pipeline.py`)
- [x] Script de demonstração interativa (`scripts/demo_pipeline.py`)
- [x] Teste end-to-end: 4/4 perguntas corretas, 0 repair attempts

### Arquivos criados

```
src/
  db/
    __init__.py
    connection.py       ← DatabaseConnection (thread-safe) + get_connection()
    schema.py           ← get_schema_info() + build_schema_prompt()
  text2sql/
    __init__.py
    domain_dict.py      ← DOMAIN_DICT (40 entradas) + get_relevant_entries()
    nl_sql_pairs.py     ← NL_SQL_PAIRS (30 pares) + get_few_shot_examples()
    pipeline.py         ← Text2SQLPipeline.run() — 5 etapas
scripts/
  validate_connection.py
  demo_pipeline.py
```

### O que foi implementado e por quê

#### `src/db/connection.py` — Camada de conexão
- **`DatabaseConnection`**: wrapper com context manager obrigatório, garante fechamento mesmo sob exceção. `read_only=True` por padrão — princípio do menor privilégio, agente nunca deve escrever.
- **`get_connection()`**: factory como context manager de alto nível para uso simples em qualquer módulo.
- Parser de URI `duckdb:////path` com normalização de barras múltiplas (formato usado no `.env`).

#### `src/db/schema.py` — Schema semântico
- **`get_schema_info()`**: extrai DDL real do banco + anota com descrições semânticas manuais + FKs explícitos. Não depende de metadados automáticos do DuckDB, que não modelam FKs.
- **`build_schema_prompt()`**: serializa o schema em formato DDL-like legível por LLMs (~5.024 chars). Incluir o schema completo no prompt da Fase 1 é intencional — na Fase 2 será substituído por schema linking seletivo.
- `TABLE_DESCRIPTIONS` e `FOREIGN_KEYS` são dicionários estáticos que capturam conhecimento do domínio que não existe no banco (ex: `MUNIC_RES` vs `MUNIC_MOV`, `codigo_6d` como FK correta).

#### `src/text2sql/domain_dict.py` — Dicionário de domínio
- 40 entradas cobrindo os maiores riscos de alucinação deste banco específico.
- Categorias: desfecho (morte), diagnóstico, custo, tempo, sexo, localização, hospital, especialidade, procedimentos, raça, vínculo previdenciário, socioeconômico, obstétrico.
- **Por que necessário**: `SEXO=3` para feminino, `MORTE::INT`, `VAL_TOT` vs `VAL_SH`, `MUNIC_RES` vs `MUNIC_MOV` — erros silenciosos que nenhum validador sintático captura.
- `get_relevant_entries()` usa matching por palavras-chave. Na Fase 2 será substituído por busca semântica via embeddings.

#### `src/text2sql/nl_sql_pairs.py` — Pares NL-SQL para few-shot
- 30 pares organizados em 3 níveis de dificuldade: simples (8), médio (14), difícil (8).
- Cobrem: contagens, filtros temporais, filtros demográficos, filtros clínicos, ranking, séries temporais, cruzamentos socioeconômicos, comparações geográficas, análises obstétricas, análise pandemia.
- **Por que few-shot é crítico aqui**: o banco usa convenções não-padrão (códigos numéricos, colunas em maiúscula, formato long no `socioeconomico`) que o LLM não infere corretamente sem exemplos.
- `get_few_shot_examples()` seleciona os 3 mais similares por matching léxico. Na Fase 2 será embedding-based.

#### `src/text2sql/pipeline.py` — Pipeline end-to-end
- **5 etapas**: schema linking → context builder → LLM → validação sintática → execução segura.
- **Repair loop**: até 3 tentativas, passando o erro anterior no prompt para auto-correção (padrão DIN-SQL).
- **Validação sintática**: `sqlglot.parse_one(sql, dialect="duckdb")` — valida sem conexão ao banco, mais rápido que `EXPLAIN`.
- **Execução segura**: bloqueia INSERT/UPDATE/DELETE/DROP, injeta `LIMIT 500` automaticamente se ausente, `read_only=True`.
- **Modelo**: `gpt-4o-mini`, `temperature=0` — determinismo máximo para SQL.

### Resultados dos testes

| Pergunta | Latência | Tokens | Repairs |
|---|---|---|---|
| Quantas internações em 2022? | 3,2s | 2.085 | 0 |
| Top 5 diagnósticos no MA? | 4,5s | 2.291 | 0 |
| Taxa de mortalidade por especialidade? | 2,7s | 2.497 | 0 |
| Custo médio RS vs MA por ano? | 2,7s | 2.461 | 0 |

**Acurácia: 4/4 (100%) | Repairs: 0/4**

### Lições aprendidas

- DuckDB lê `SEXO=3` como feminino — sem o dicionário de domínio, o LLM geraria `SEXO=2` em 100% dos casos.
- `MORTE::INT` precisa ser explicitado — `SUM(MORTE)` falha silenciosamente em DuckDB com BOOLEANs.
- O schema completo (~5k chars) cabe no contexto do `gpt-4o-mini` sem degradação visível. A partir de schemas maiores ou contextos maiores, schema linking seletivo passa a ser obrigatório.
- Few-shot com 3 exemplos relevantes foi suficiente para queries de nível médio sem nehum repair.

---

## Fase 2 — Qualidade ✅

**Objetivo:** Elevar a robustez e rastreabilidade do sistema. Schema linking semântico via embeddings, avaliação automática com execution accuracy, logging estruturado.

**Status:** Concluída e validada em 07/03/2026.

### Checklist

- [x] Embeddings para schema linking semântico
  - [x] Indexar todas as colunas + descrições + exemplos de valores (91 docs)
  - [x] Substituir matching léxico por busca por similaridade coseno
  - [x] Selecionar subschema relevante (não schema completo) por query
- [x] Embeddings para few-shot selection
  - [x] Embedar os 30 pares NL-SQL existentes
  - [x] Selecionar exemplos por similaridade semântica à query
- [x] Framework de avaliação automática
  - [x] Implementar Execution Accuracy (EX) metric sobre os pares anotados
  - [x] Script de avaliação em batch (`scripts/evaluate.py`)
  - [x] Relatório de acurácia por categoria (simples/médio/difícil)
  - [x] Baseline com os 30 pares da Fase 1
- [x] Observability com logger de traces
  - [x] Logger JSON Lines integrado ao pipeline (`logs/traces.jsonl`)
  - [x] Logar: question, sql, tokens, latência, sucesso, repair attempts, tabelas usadas
  - [x] `logger.summary()` para métricas agregadas do histórico
- [x] Melhorias no repair loop e system prompt
  - [x] Identificar padrões de erro (geographic over-filtering)
  - [x] Corrigir system prompt: instrução explícita de não adicionar filtros geográficos não pedidos
  - [x] Comparador EX com tolerância para floats e subset matching

### Arquivos criados

```
src/text2sql/
  vector_store.py     ← EmbeddingStore (OpenAI text-embedding-3-small + numpy cosine sim)
  schema_linker.py    ← build_schema_index() + link_schema() — schema seletivo por query
  few_shot_store.py   ← build_few_shot_index() + get_similar_examples() — few-shot semântico
  logger.py           ← TraceLogger (context manager, JSON Lines, summary())
scripts/
  build_indexes.py    ← constrói e persiste índices (.vector_store/*.json)
  evaluate.py         ← avaliação EX em batch + relatório JSON
.vector_store/
  schema_columns.json ← índice persistido (91 docs)
  few_shot_pairs.json ← índice persistido (30 docs)
logs/
  traces.jsonl        ← histórico de execuções
reports/
  eval_*.json         ← relatórios de avaliação
```

### O que foi implementado e por quê

#### `src/text2sql/vector_store.py` — Vector Store sem dependências pesadas
- Implementado com `numpy` + `openai` embeddings — sem ChromaDB ou pgvector.
- **Decisão**: ChromaDB tem dependências pesadas (onnxruntime). Para um banco de 91+30 docs, numpy é 10x mais simples e igualmente rápido. ChromaDB só faria sentido com >10k docs.
- `EmbeddingStore`: add → build → search. Persiste em JSON, re-embedda apenas docs novos (incremental).
- Modelo: `text-embedding-3-small` (1536 dims, custo ~$0.0001 por 1k tokens).
- Construção do índice completo: 3.8s para 121 docs. Busca: <1ms por query (operação numpy em memória).

#### `src/text2sql/schema_linker.py` — Schema Linking Semântico
- Indexa cada coluna com texto enriquecido: nome, notas críticas de domínio, sinônimos, tipo.
- `link_schema()` retorna subschema com apenas as tabelas relevantes + seus FKs.
- Sempre inclui `internacoes` (tabela fato central) independente do resultado.
- Threshold de similaridade: 0.25 (coseno) para incluir uma tabela.
- **Resultado**: tokens no prompt caíram de ~5.000 para ~1.200-1.800 (redução de 64-76%).

#### `src/text2sql/few_shot_store.py` — Few-Shot Semântico
- Indexa as perguntas dos 30 pares NL-SQL + suas tags.
- `get_similar_examples()` retorna os 3 exemplos com maior similaridade coseno.
- Fallback automático para matching léxico se o índice não estiver construído.

#### `src/text2sql/logger.py` — Logger de Traces
- Context manager: `with logger.trace(question) as t:` — persiste mesmo sob exceção.
- Cada span registra o tempo, dados de debug e a etapa do pipeline.
- `summary()` agrega métricas históricas: taxa de sucesso, latência média, tokens médios.
- **Decisão**: Langfuse requer servidor (Docker). Logger JSON Lines é zero-infra, portável e suficiente para a fase de desenvolvimento. Migração para Langfuse é uma troca de 1 arquivo.

#### `pipeline.py` — Atualizado para Fase 2
- Recebe `schema_store` e `few_shot_store` opcionais no construtor.
- Fallback gracioso para comportamento Fase 1 se índices não disponíveis.
- Logger integrado ao `run()` via context manager.
- System prompt corrigido: instrução explícita anti-geographic-overfitting.

#### `scripts/evaluate.py` — Framework de Avaliação EX
- Execution Accuracy (EX): padrão BIRD benchmark.
- Comparador com 4 estratégias em ordem: set equality → scalar → subset gold⊆pred → subset pred⊆gold.
- Tolerância de 1 casa decimal em floats (absorve imprecisão de ponto flutuante em SUM de 18M linhas).
- Salva relatório JSON com falhas detalhadas para análise.

### Resultados finais da Fase 2

| Métrica | Valor |
|---|---|
| EX Accuracy (30 pares) | **100%** (30/30) |
| EX simples (8 pares) | 100% |
| EX médio (13 pares) | 100% |
| EX difícil (9 pares) | 100% |
| Repairs necessários | 0 |
| Latência média | ~2.800ms |
| Tokens médios por query | ~1.579 (↓64% vs Fase 1) |
| Build dos índices | 3.8s |

### Lições aprendidas

- **Geographic over-filtering**: o LLM lê "banco cobre RS e MA" no system prompt e adiciona `WHERE estado='RS'` mesmo sem ser pedido. Solução: instrução negativa explícita com exemplos ERRADO/CERTO no system prompt.
- **Float imprecisão em SUM**: `ROUND(SUM(VAL_TOT), 2)` sobre 18M linhas pode divergir em $0.01 dependendo da ordem de execução. Comparar com arredondamento de 1 casa decimal resolve.
- **Subset matching > set equality**: gold SQL com LIMIT 10, pred SQL com LIMIT 500 — resultados são subconjuntos válidos um do outro. Comparação set equality pura geraria falsos negativos.
- **Threshold de schema linking**: 0.25 coseno funciona bem. Abaixo de 0.20 inclui tabelas irrelevantes; acima de 0.35 pode excluir tabelas necessárias para JOINs.
- **Tags enriquecem few-shot**: indexar `"pergunta [tags: ranking, série temporal, CID]"` melhora o recall semântico para perguntas que usam vocabulário diferente do exemplo.

---

## Fase 3 — Produto ✅

**Objetivo:** Expor o pipeline como API REST consumível e interface de chat simples.

**Status:** Concluída e validada em 07/03/2026.

### Checklist

- [x] API REST com FastAPI
  - [x] Endpoint `POST /query` — recebe pergunta, retorna SQL + resultado
  - [x] Endpoint `GET /health` — status do banco e pipeline
  - [x] Endpoint `GET /examples` — lista perguntas de exemplo
  - [x] Endpoint `GET /traces/summary` — métricas históricas
  - [x] Pydantic v2 para request/response models
  - [x] Tratamento de erros com códigos HTTP adequados (200/504/503/500)
  - [x] Timeout de 60s por request via asyncio
  - [x] CORS middleware configurado
  - [x] Swagger UI automático em `/docs`
- [x] Interface de chat Streamlit
  - [x] Input de pergunta em linguagem natural
  - [x] Exibição do SQL gerado (expansível)
  - [x] Exibição do resultado em `st.dataframe`
  - [x] Histórico completo da sessão com métricas por query
  - [x] Sidebar com exemplos organizados por dificuldade (clicáveis)
  - [x] Detalhes técnicos expansíveis (tabelas usadas, schema selecionado)
  - [x] Modo dual: API (preferencial) + standalone (fallback automático)
- [x] Documentação
  - [x] README.md com quickstart completo
  - [x] Exemplos de perguntas suportadas (simples/médio/difícil)
  - [x] Tabela de limitações conhecidas do sistema
  - [x] Tabela de scripts disponíveis

### Arquivos criados

```
src/api/
  __init__.py
  models.py     ← Pydantic v2 QueryRequest, QueryResponse, HealthResponse, etc.
  startup.py    ← lifespan + AppState singleton (pipeline carregado 1x no startup)
  app.py        ← FastAPI app + 4 endpoints + CORS + exception handler
scripts/
  serve.py      ← uvicorn launcher (--port, --reload)
  chat.py       ← Streamlit interface
README.md
```

### O que foi implementado e por quê

#### `src/api/startup.py` — Lifespan + AppState singleton
- Pipeline é caro de instanciar (carrega índices de embeddings em memória). Sem o padrão singleton via `lifespan`, cada request recriaria o pipeline.
- `AppState` é um dataclass com o pipeline, os dois stores e o logger — compartilhado entre todos os requests via `get_state()`.
- **Decisão**: usar `lifespan` (FastAPI 0.93+) em vez de `@app.on_event("startup")` deprecated.

#### `src/api/app.py` — FastAPI app
- Endpoints síncronos executados via `loop.run_in_executor(threadpool)` com `asyncio.wait_for()` — DuckDB não é async-native, mas o uvicorn não bloqueia outros requests durante a execução.
- ThreadPoolExecutor com `max_workers=4` — suporte a 4 queries simultâneas (banco é read-only, sem conflito de lock).
- Erros de negócio (`success=False`) retornam HTTP 200 com payload descritivo — não 500. Falhas de infraestrutura retornam 503.
- CORS `allow_origins=["*"]` — adequado para desenvolvimento; restringir em produção.

#### `scripts/chat.py` — Streamlit
- **Modo dual**: tenta `POST /api` com timeout de 2s; se falhar, carrega pipeline diretamente (`st.session_state.pipeline` para não recarregar a cada interação).
- `st.session_state.history` persiste o histórico da sessão inteira, não apenas a última query.
- Exemplos na sidebar são botões que populam o input via `st.session_state.pending_question` — padrão recomendado para evitar re-renders duplos no Streamlit.
- `pd.DataFrame` para exibição — converte automaticamente tipos DuckDB para pandas.

### Resultados dos testes

| Endpoint | Status | Tempo de resposta |
|---|---|---|
| `GET /health` | 200 OK | <100ms |
| `POST /query` ("Quantas internações em 2023?") | 200 OK | 3.103ms |
| `GET /examples` | 200 OK | <10ms |
| `GET /traces/summary` | 200 OK | <50ms |

Pipeline singleton carregado no startup: índices em memória, zero latência de I/O por request.

### Lições aprendidas

- **Threadpool + asyncio.wait_for**: combinar `run_in_executor` com `wait_for` é o padrão correto para chamar código síncrono (DuckDB) de dentro de um endpoint async sem bloquear o event loop do uvicorn.
- **AppState via `lifespan`**: injetar o pipeline como estado global evita o anti-pattern de importar objetos pesados no escopo do módulo, que causaria problemas com `--reload`.
- **Streamlit modo dual**: carregar o pipeline standalone diretamente no Streamlit (sem API) é útil para demos rápidos sem precisar subir dois processos.

---

## Fase 4 — Multiagente ✅

**Objetivo:** Evoluir para arquitetura multiagente com LangGraph para suportar queries complexas, decomposição automática e respostas em linguagem natural.

**Status:** Concluída e validada em 07/03/2026.

### Checklist

- [x] Migrar pipeline para LangGraph
  - [x] Modelar etapas como nós do grafo
  - [x] Repair loop como aresta cíclica (não recursão)
  - [x] State management explícito entre nós (`AgentState` TypedDict)
- [x] Agente Classifier (Orchestrator)
  - [x] Classifica query como "simple" ou "complex" via LLM (JSON response)
  - [x] Gera `complexity_reason` descrevendo a decisão
  - [x] Roteia para `schema_link` ou `decompose` conforme classificação
- [x] Agente Decomposer
  - [x] Quebra perguntas complexas em 2-4 sub-queries independentes
  - [x] Executa cada sub-query via `Text2SQLPipeline` existente (Fase 2)
  - [x] Agrega resultados parciais em `sub_questions` + `sub_results`
- [x] Agente Validator (sintático + semântico)
  - [x] `validate_syntax_node`: sqlglot offline (reutilizado da Fase 2)
  - [x] `validate_result_node`: 4 checagens semânticas — taxa > 100%, resultado vazio, ano fora do range 2008–2023, custo absurdo (>R$1M/internação)
  - [x] Popula `semantic_warnings` com avisos sem bloquear o fluxo
- [x] Agente Explainer
  - [x] Recebe dados brutos (rows + columns) e gera resposta em português
  - [x] Funciona nos dois paths: simples (tabela de resultado) e complexo (sub-resultados narrativos)
  - [x] `temperature=0.3` para respostas naturais mas reprodutíveis
- [x] Nó de reparação (`repair_node`)
  - [x] Incrementa `repair_attempts` e passa `last_error` para o contexto do LLM
  - [x] Limite de 3 tentativas — excedido rota para `end_error_node`
- [x] Integração com a API REST (Fase 3)
  - [x] Endpoint `POST /agent/query` adicionado ao `app.py`
  - [x] `AppState` atualizado para carregar `agent_graph` no startup
  - [x] Modelos Pydantic v2 `AgentQueryResponse` e `SubResult` adicionados
- [x] Script de demonstração (`scripts/demo_agent.py`)
  - [x] 5 perguntas de teste (3 simples + 2 complexas)
  - [x] Exibe tipo de query, SQL, sub-queries, warnings e resposta NL

### Arquivos criados

```
src/agent/
  __init__.py
  state.py        ← AgentState (TypedDict, total=False) com todos os campos do grafo
  nodes.py        ← 9 funções de nó: classify, schema_link, generate_sql,
                    validate_syntax, execute, validate_result, repair,
                    decompose (factory), explain
  routing.py      ← 5 funções de roteamento condicional
  graph.py        ← build_graph() + run_agent() + AgentResult dataclass
scripts/
  demo_agent.py   ← demo das 5 perguntas de teste
src/api/
  models.py       ← + AgentQueryResponse, SubResult (adicionados)
  startup.py      ← + agent_graph no AppState (atualizado)
  app.py          ← + POST /agent/query (atualizado)
```

### O que foi implementado e por quê

#### `src/agent/state.py` — AgentState
- `TypedDict` com `total=False` — todos os campos são opcionais. Nós só retornam os campos que modificam, o LangGraph faz merge automático no estado global.
- Campos acumulativos: `repair_attempts`, `sql_attempts`, `total_tokens` — somados incrementalmente pelos nós.
- `semantic_warnings: list[str]` — acumula avisos de múltiplos validadores sem sobrescrever.

#### `src/agent/nodes.py` — 9 nós de processamento
- **`classify_node`**: usa `gpt-4o-mini` com prompt JSON-only. Distingue "complex" quando há comparação entre grupos, análise multidimensional ou agregação cruzada. `temperature=0` para classificação determinística.
- **`make_schema_link_node()`**: factory que fecha sobre `schema_store` e `few_shot_store`. Reutiliza `link_schema()` e `get_similar_examples()` da Fase 2 — zero duplicação de código.
- **`generate_sql_node`**: reutiliza exatamente o mesmo system prompt da Fase 2 (`Text2SQLPipeline`). Incrementa `sql_attempts` antes de chamar o LLM.
- **`validate_syntax_node`**: sqlglot offline, sem conexão ao banco. Popula `last_error` com mensagem formatada para o repair.
- **`execute_node`**: DuckDB seguro (blocklist + LIMIT 500 + read_only). Extrai `tables_used` do SQL via regex para rastreabilidade.
- **`validate_result_node`**: 4 heurísticas de sanidade. Avisos em `semantic_warnings` não bloqueiam — o Explainer pode mencionar as limitações na resposta NL.
- **`repair_node`**: incrementa `repair_attempts`. O roteamento verifica se `repair_attempts >= MAX_REPAIRS` (3) e rota para `end_error`.
- **`make_decompose_node()`**: factory. LLM gera JSON com lista de sub-perguntas. Cada sub-pergunta é executada pela `Text2SQLPipeline` — reutiliza todo o pipeline Fase 2 incluindo repair loop interno.
- **`explain_node`**: dois paths internos — simples (descreve a tabela de resultado em prosa) e complexo (sintetiza os sub-resultados em análise narrativa).

#### `src/agent/routing.py` — Roteamento condicional
- **`route_after_classify`**: "simple" → `schema_link`; "complex" → `decompose`.
- **`route_after_validate_syntax`**: sem erro → `execute`; com erro + tentativas disponíveis → `repair`; tentativas esgotadas → `end_error`.
- **`route_after_execute`**: sucesso → `validate_result`; falha → `repair` ou `end_error`.
- **`route_after_validate_result`**: sempre → `explain` (warnings não bloqueiam). Se resultado vazio → `repair`.
- **`route_after_repair`**: sempre → `generate_sql` (o nó de repair não gera SQL, apenas prepara o contexto).

#### `src/agent/graph.py` — Topologia do grafo
- `build_graph()` retorna grafo compilado. Aceita `schema_store` e `few_shot_store` opcionais (se `None`, chama os builders).
- `run_agent()` é a entry point pública. Cria `initial_state` minimal, invoca o grafo, extrai `AgentResult` do `final_state`.
- `end_error_node`: nó terminal especial — ativado apenas quando `repair_attempts >= 3`. Garante que o grafo sempre termina mesmo sob falhas consecutivas.
- Topologia final:
  ```
  START → classify → schema_link → generate_sql → validate_syntax
                                                        │ ok
                                                     execute
                                                        │ ok
                                                  validate_result → explain → END
                                                        ↑
           classify → decompose ──────────────────────┘
  repair loop: validate_syntax/execute/validate_result → repair → generate_sql
  ```

#### Decisão: Decomposer sequencial (não paralelo)
- O plano original previa execução paralela de sub-queries (LangGraph `Send` API).
- **Decisão**: execução sequencial dentro do `decompose_node` usando a `Text2SQLPipeline` existente.
- **Justificativa**: DuckDB tem lock de leitura em modo read-only que pode conflitar com múltiplas threads simultâneas. Sequencial é mais seguro e igualmente correto — sub-queries são independentes mas não críticas em latência.

#### Integração com API (Fase 3)
- `AppState` agora carrega `agent_graph` no startup via `build_graph(schema_store, few_shot_store)` — grafo compilado 1x e compartilhado.
- `POST /agent/query` usa `timeout=REQUEST_TIMEOUT_SECONDS * 2` (120s) — agente multi-LLM é mais lento que o pipeline direto.
- `AgentQueryResponse` expõe todos os campos do `AgentResult` incluindo `sub_questions` e `sub_results` para inspeção do processo de decomposição.

### Resultados dos testes (demo_agent.py)

| Pergunta | Tipo | Latência | Tokens | Repairs |
|---|---|---|---|---|
| Quantas internações em 2022? | SIMPLE | 4.688ms | 1.744 | 0 |
| Top 5 diagnósticos no MA? | SIMPLE | 7.220ms | 2.519 | 0 |
| Taxa de mortalidade por especialidade? | SIMPLE | 6.244ms | 3.020 | 0 |
| Compare mortalidade e custo RS vs MA | COMPLEX | 11.450ms | — | 0 (2 sub-queries) |
| Mortalidade IDH alto vs baixo? | COMPLEX | 14.150ms | — | 0 (2 sub-queries) |

**Acurácia: 5/5 (100%) | Repairs: 0/5 | Sub-queries: 4 executadas com sucesso**

Todas as 5 perguntas retornaram `success=True` com resposta em linguagem natural do Explainer.

### Lições aprendidas

- **`total=False` no TypedDict é essencial**: sem isso, o LangGraph exige que todos os campos sejam inicializados no `initial_state`. Com `total=False`, nós retornam apenas os campos que alteram — o merge é feito automaticamente.
- **Factory pattern para nós com dependências**: nós que precisam de `schema_store` ou `few_shot_store` são criados como closures via funções factory (`make_schema_link_node`, `make_decompose_node`). Isso mantém a assinatura dos nós compatível com o LangGraph (`state → dict`) sem passar dependências via estado.
- **Reutilização da Fase 2 no Decomposer**: executar sub-queries via `Text2SQLPipeline` em vez de implementar um pipeline paralelo separado foi a decisão correta — o repair loop interno da Fase 2 já trata erros de sub-queries automaticamente.
- **`end_error_node` como nó terminal explícito**: LangGraph não permite loops infinitos sem saída. Ter um nó `end_error` explícito conectado ao `END` garante terminação mesmo quando `repair_attempts >= 3`.
- **Classify com JSON-only**: pedir ao LLM para responder exclusivamente em JSON (sem markdown, sem explicações) e usar `json.loads()` com fallback para "simple" torna a classificação robusta a variações de formato.

---

## Decisões Arquiteturais Registradas

| Decisão | Alternativas consideradas | Justificativa |
|---|---|---|
| DuckDB `read_only=True` por padrão | Conexão R/W | Princípio do menor privilégio — agente nunca deve modificar dados |
| `sqlglot` para validação sintática | DuckDB `EXPLAIN` | Sqlglot não precisa de conexão, valida offline, útil em batch |
| `gpt-4o-mini`, `temperature=0` | Modelos maiores, temperatura > 0 | Custo-benefício: precisão suficiente, determinismo máximo para SQL |
| Schema completo no prompt (Fase 1) | Schema seletivo | Fase 1 é baseline — schema completo cabe em contexto e simplifica implementação |
| Few-shot por matching léxico (Fase 1) | Embedding semântico | Suficiente para baseline, substituído na Fase 2 |
| numpy + JSON para vector store (Fase 2) | ChromaDB, pgvector, Pinecone | 91+30 docs não justificam dependência pesada; numpy cosine sim é <1ms e zero infra |
| Decomposer sequencial (Fase 4) | LangGraph Send API (paralelo) | DuckDB read-only pode conflitar com múltiplas threads; sequencial é correto e mais simples |
| Factory pattern para nós com stores (Fase 4) | Injeção via estado | Mantém assinatura `state → dict` compatível com LangGraph sem poluir o AgentState |
| LangGraph para multiagente (Fase 4) | LangChain chains, CrewAI | Grafos cíclicos com state management explícito — modelagem correta do repair loop e fan-out |
| Langfuse para observability | Langsmith, W&B | Open-source, auto-hospedável, sem lock-in de vendor |

---

## Riscos Monitorados

| Risco | Mitigação implementada | Status |
|---|---|---|
| `SEXO=3` para feminino — erro silencioso | Regra no system prompt + entrada no domain_dict | ✅ Mitigado |
| `MORTE::INT` necessário para aggregation | Regra no system prompt + pares NL-SQL com exemplos | ✅ Mitigado |
| Contagem via `atendimentos` em vez de `internacoes` | Regra explícita no system prompt | ✅ Mitigado |
| `VAL_TOT` vs `VAL_SH`/`VAL_SP` | Entrada no domain_dict + exemplos | ✅ Mitigado |
| `MUNIC_RES` vs `MUNIC_MOV` | Entrada no domain_dict com nota detalhada | ✅ Mitigado |
| Banco = RS+MA apenas — LLM pode generalizar | Aviso no system prompt e no domain_dict | ✅ Mitigado |
| Queries lentas sem LIMIT | LIMIT 500 injetado automaticamente na execução | ✅ Mitigado |
| Operações de escrita acidentais | Blocklist explícita (INSERT/UPDATE/DELETE/DROP) | ✅ Mitigado |
| `socioeconomico` long format — LLM esquece filtro `metrica` | Nota no domain_dict + exemplos few-shot com CTE | ✅ Mitigado (testado na avaliação EX) |
| Schema linking falha em queries ambíguas | Fase 2 — embeddings semânticos (threshold 0.25) | ✅ Mitigado |
| Sem métrica objetiva de qualidade | Fase 2 — EX metric sobre 30 pares (100% acurácia) | ✅ Mitigado |
| Queries complexas sem decomposição falham | Fase 4 — Decomposer + LangGraph multiagente | ✅ Mitigado |
| Sem resposta em linguagem natural | Fase 4 — Explainer com gpt-4o-mini | ✅ Mitigado |
