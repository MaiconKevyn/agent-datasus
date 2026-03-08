"""
Pipeline Text-to-SQL — Fase 2

Melhorias sobre a Fase 1:
  - Schema linking semântico (embeddings) em vez de matching léxico
  - Few-shot selection semântico (embeddings) em vez de matching léxico
  - Schema seletivo no prompt (só tabelas relevantes, ~50% menos tokens)
  - Logger de traces estruturado (JSON Lines)

Fluxo:
  pergunta NL
    → schema linking semântico (EmbeddingStore)
    → few-shot semântico (EmbeddingStore)
    → context builder (subschema + regras + few-shot)
    → LLM (gpt-4o-mini, temperature=0)
    → validação sintática (sqlglot)
    → repair loop (até MAX_REPAIR_ATTEMPTS, erro no prompt)
    → execução segura (read-only, LIMIT, blocklist DDL/DML)
    → logger (traces.jsonl)
    → resultado
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from dotenv import load_dotenv
from openai import OpenAI

from src.db.connection import get_connection
from src.text2sql.domain_dict import get_relevant_entries, format_domain_rules
from src.text2sql.few_shot_store import EmbeddingStore as FewShotStore, get_similar_examples
from src.text2sql.logger import TraceLogger
from src.text2sql.nl_sql_pairs import format_few_shot
from src.text2sql.schema_linker import link_schema

_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

MODEL = "gpt-4o-mini"
TEMPERATURE = 0.0
MAX_REPAIR_ATTEMPTS = 3
MAX_ROWS = 500

SYSTEM_PROMPT = """Você é um especialista em SQL para o banco SIH-RD do DATASUS.
O banco contém dados de internações hospitalares do SUS nos estados RS e MA, período 2008-2023.
Use EXCLUSIVAMENTE DuckDB SQL.

REGRAS CRÍTICAS — NUNCA IGNORE:
1. SEXO: Masculino=1, Feminino=3 (não existe valor 2).
2. MORTE é BOOLEAN. Para filtrar óbitos: MORTE=TRUE. Para somar: SUM(MORTE::INT).
3. Para CONTAR internações: sempre use a tabela 'internacoes', nunca 'atendimentos'.
4. Para custo total: VAL_TOT (não VAL_SH nem VAL_SP).
5. Para município do paciente: MUNIC_RES → municipios.codigo_6d.
   Para município do hospital: hospital.MUNIC_MOV → municipios.codigo_6d.
6. O banco contém dados de RS e MA. NÃO adicione filtros geográficos (estado, município)
   que não foram pedidos explicitamente na pergunta.
   - Pergunta SEM menção a estado/município → NÃO faça JOIN com municipios, NÃO filtre por estado.
   - Pergunta COM "no RS", "no MA", "no Maranhão", "no Rio Grande do Sul" → filtre por estado.
   - ERRADO: "SELECT ... FROM internacoes JOIN municipios WHERE estado='RS'" quando não pedido.
   - CERTO:  "SELECT ... FROM internacoes" quando a pergunta não pede recorte geográfico.
7. 'socioeconomico' é formato long: sempre filtre por metrica (ex: metrica = 'idhm').
8. Se a pergunta pedir "top N" ou "N maiores/menores", use LIMIT N. Caso contrário, LIMIT 500.
9. Retorne APENAS o SQL, sem explicações, sem markdown, sem comentários.
10. Use alias descritivos nas colunas (AS nome_coluna).
"""


# ---------------------------------------------------------------------------
# Resultado do pipeline
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    question: str
    sql: str
    rows: list[tuple]
    columns: list[str]
    success: bool
    error: str | None = None
    repair_attempts: int = 0
    latency_ms: int = 0
    tokens_used: dict[str, int] = field(default_factory=dict)
    tables_used: list[str] = field(default_factory=list)
    schema_tables_selected: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

class Text2SQLPipeline:
    def __init__(
        self,
        schema_store: EmbeddingStore | None = None,
        few_shot_store: FewShotStore | None = None,
    ) -> None:
        """
        Args:
            schema_store: Índice de schema pré-construído (EmbeddingStore).
                          Se None, usa schema completo como fallback.
            few_shot_store: Índice de pares NL-SQL pré-construído.
                            Se None, usa matching léxico como fallback.
        """
        self._client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self._schema_store = schema_store
        self._few_shot_store = few_shot_store
        self._logger = TraceLogger()

    # ── Etapa 1: Schema linking ──────────────────────────────────────────────

    def _link_schema(self, question: str) -> tuple[str, list[str]]:
        """
        Retorna (schema_prompt, tabelas_selecionadas).
        Se schema_store disponível: usa embeddings. Senão: schema completo.
        """
        if self._schema_store and self._schema_store.is_built:
            return link_schema(question, self._schema_store)

        # Fallback: schema completo (comportamento Fase 1)
        from src.db.schema import build_schema_prompt, get_schema_info
        all_tables = get_schema_info()
        return build_schema_prompt(all_tables), [t.name for t in all_tables]

    def _get_domain_rules(self, question: str) -> str:
        entries = get_relevant_entries(question)
        return format_domain_rules(entries)

    # ── Etapa 2: Few-shot selection ──────────────────────────────────────────

    def _get_few_shot(self, question: str) -> str:
        if self._few_shot_store and self._few_shot_store.is_built:
            examples = get_similar_examples(question, self._few_shot_store, n=3)
        else:
            # Fallback: matching léxico (comportamento Fase 1)
            from src.text2sql.nl_sql_pairs import get_few_shot_examples
            examples = get_few_shot_examples(question, n=3)
        return format_few_shot(examples)

    # ── Etapa 3: Context builder ─────────────────────────────────────────────

    def _build_prompt(
        self,
        question: str,
        schema_prompt: str,
        domain_rules: str,
        few_shot: str,
        error: str | None = None,
    ) -> list[dict]:
        repair_block = ""
        if error:
            repair_block = f"\nERRO NA TENTATIVA ANTERIOR (corrija o SQL):\n{error}\n"

        user_content = (
            f"SCHEMA DO BANCO:\n{schema_prompt}\n\n"
            f"{domain_rules}\n\n"
            f"EXEMPLOS DE REFERÊNCIA:\n{few_shot}\n"
            f"PERGUNTA: {question}\n"
            f"{repair_block}"
            "SQL:"
        )
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

    # ── Etapa 4: Geração SQL via LLM ─────────────────────────────────────────

    def _generate_sql(self, messages: list[dict]) -> tuple[str, dict]:
        response = self._client.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=TEMPERATURE,
        )
        sql = response.choices[0].message.content.strip()
        tokens = {
            "input": response.usage.prompt_tokens,
            "output": response.usage.completion_tokens,
            "total": response.usage.total_tokens,
        }
        return sql, tokens

    # ── Etapa 5: Validação sintática ─────────────────────────────────────────

    def _validate_syntax(self, sql: str) -> str | None:
        try:
            sqlglot.parse_one(sql, dialect="duckdb")
            return None
        except sqlglot.errors.ParseError as e:
            return f"Erro de sintaxe SQL: {e}"

    # ── Etapa 6: Execução segura ──────────────────────────────────────────────

    def _execute_safe(self, sql: str) -> tuple[list[tuple], list[str], str | None]:
        sql_upper = sql.upper()
        for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"):
            if forbidden in sql_upper:
                return [], [], f"Operação '{forbidden}' não permitida."

        if "LIMIT" not in sql_upper:
            sql = sql.rstrip(";") + f"\nLIMIT {MAX_ROWS};"

        try:
            with get_connection(read_only=True) as conn:
                result = conn.execute(sql)
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description] if result.description else []
            return rows, columns, None
        except Exception as e:
            return [], [], str(e)

    # ── Pipeline completo ─────────────────────────────────────────────────────

    def run(self, question: str) -> PipelineResult:
        t0 = time.monotonic()
        total_tokens: dict[str, int] = {"input": 0, "output": 0, "total": 0}

        with self._logger.trace(question) as trace:

            # Etapas 1+2: schema linking e few-shot (fora do loop de repair)
            schema_prompt, selected_tables = self._link_schema(question)
            domain_rules = self._get_domain_rules(question)
            few_shot = self._get_few_shot(question)

            trace.log_span("schema_linking", tables_selected=selected_tables)

            sql = ""
            error: str | None = None
            repair_attempts = 0

            for attempt in range(MAX_REPAIR_ATTEMPTS + 1):

                # Etapa 3+4: monta prompt e gera SQL
                messages = self._build_prompt(
                    question, schema_prompt, domain_rules, few_shot,
                    error if attempt > 0 else None,
                )
                try:
                    sql, tokens = self._generate_sql(messages)
                except Exception as e:
                    result = PipelineResult(
                        question=question, sql="", rows=[], columns=[],
                        success=False, error=f"Erro na chamada ao LLM: {e}",
                        latency_ms=int((time.monotonic() - t0) * 1000),
                        schema_tables_selected=selected_tables,
                    )
                    trace.set_result(success=False, error=result.error,
                                     latency_ms=result.latency_ms)
                    return result

                for k in total_tokens:
                    total_tokens[k] += tokens.get(k, 0)
                trace.log_span("llm_call", attempt=attempt, tokens=tokens, sql_preview=sql[:120])

                # Etapa 5: validação sintática
                syntax_error = self._validate_syntax(sql)
                if syntax_error:
                    error = syntax_error
                    repair_attempts += 1
                    trace.log_span("syntax_error", error=error, attempt=attempt)
                    if attempt < MAX_REPAIR_ATTEMPTS:
                        continue
                    result = PipelineResult(
                        question=question, sql=sql, rows=[], columns=[],
                        success=False, error=error, repair_attempts=repair_attempts,
                        latency_ms=int((time.monotonic() - t0) * 1000),
                        tokens_used=total_tokens, schema_tables_selected=selected_tables,
                    )
                    trace.set_result(success=False, sql=sql, error=error,
                                     repair_attempts=repair_attempts,
                                     latency_ms=result.latency_ms, tokens=total_tokens,
                                     schema_tables_selected=selected_tables)
                    return result

                # Etapa 6: execução
                rows, columns, exec_error = self._execute_safe(sql)
                if exec_error:
                    error = exec_error
                    repair_attempts += 1
                    trace.log_span("execution_error", error=error, attempt=attempt)
                    if attempt < MAX_REPAIR_ATTEMPTS:
                        continue
                    result = PipelineResult(
                        question=question, sql=sql, rows=[], columns=[],
                        success=False, error=error, repair_attempts=repair_attempts,
                        latency_ms=int((time.monotonic() - t0) * 1000),
                        tokens_used=total_tokens, schema_tables_selected=selected_tables,
                    )
                    trace.set_result(success=False, sql=sql, error=error,
                                     repair_attempts=repair_attempts,
                                     latency_ms=result.latency_ms, tokens=total_tokens,
                                     schema_tables_selected=selected_tables)
                    return result

                # Sucesso
                latency = int((time.monotonic() - t0) * 1000)
                tables = _extract_table_names(sql)
                result = PipelineResult(
                    question=question, sql=sql, rows=rows, columns=columns,
                    success=True, repair_attempts=repair_attempts,
                    latency_ms=latency, tokens_used=total_tokens,
                    tables_used=tables, schema_tables_selected=selected_tables,
                )
                trace.set_result(
                    success=True, sql=sql, rows_returned=len(rows),
                    repair_attempts=repair_attempts, latency_ms=latency,
                    tokens=total_tokens, tables_used=tables,
                    schema_tables_selected=selected_tables,
                )
                return result

        # Nunca deve chegar aqui
        return PipelineResult(
            question=question, sql=sql, rows=[], columns=[],
            success=False, error="Número máximo de tentativas atingido.",
            repair_attempts=repair_attempts,
            latency_ms=int((time.monotonic() - t0) * 1000),
            tokens_used=total_tokens,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_table_names(sql: str) -> list[str]:
    try:
        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        return [t.name.lower() for t in parsed.find_all(sqlglot.exp.Table) if t.name]
    except Exception:
        return []
