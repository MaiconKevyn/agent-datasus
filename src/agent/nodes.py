"""
Nós do grafo LangGraph.

Cada função:
  - Recebe AgentState completo
  - Retorna dicionário com apenas os campos modificados (partial update)
  - É stateless — nenhum side-effect além das chamadas ao LLM/banco

Nós implementados:
  classify_node       → decide "simple" ou "complex"
  schema_link_node    → schema linking semântico + few-shot + domain rules
  generate_sql_node   → LLM gera SQL
  validate_syntax_node → sqlglot valida sem conexão
  execute_node        → DuckDB executa com segurança
  validate_result_node → sanidade semântica do resultado
  repair_node         → incrementa contador, prepara mensagem de reparo
  decompose_node      → LLM decompõe + executa sub-queries via pipeline Fase 2
  explain_node        → LLM gera resposta em linguagem natural
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import sqlglot
from dotenv import load_dotenv
from openai import OpenAI

from src.agent.state import AgentState, MAX_REPAIR_ATTEMPTS, MAX_ROWS, MODEL
from src.db.connection import get_connection
from src.text2sql.domain_dict import get_relevant_entries, format_domain_rules
from src.text2sql.nl_sql_pairs import format_few_shot
from src.text2sql.pipeline import SYSTEM_PROMPT

_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _llm(messages: list[dict], temperature: float = 0.0) -> tuple[str, dict]:
    resp = _client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=temperature,
    )
    content = resp.choices[0].message.content.strip()
    tokens = {
        "input": resp.usage.prompt_tokens,
        "output": resp.usage.completion_tokens,
        "total": resp.usage.total_tokens,
    }
    return content, tokens


def _merge_tokens(state: AgentState, new: dict) -> dict[str, int]:
    current = state.get("total_tokens") or {"input": 0, "output": 0, "total": 0}
    return {k: current.get(k, 0) + new.get(k, 0) for k in ("input", "output", "total")}


# ---------------------------------------------------------------------------
# Nó 1: Classificação
# ---------------------------------------------------------------------------

_CLASSIFY_SYSTEM = """Você é um classificador de queries Text-to-SQL para o banco SIH-RD DATASUS.
Classifique a pergunta como "simple" ou "complex".

"simple" = pode ser respondida com UMA query SQL (mesmo que tenha JOINs ou CTEs).
"complex" = requer MÚLTIPLAS queries independentes cujos resultados precisam ser
           combinados/correlacionados — ex: calcular X para o grupo A e Y para o grupo B
           separadamente, depois comparar; ou buscar dados de fontes distintas e cruzar.

Responda APENAS com JSON válido:
{"type": "simple" | "complex", "reason": "<explicação em 1 frase>"}"""


def classify_node(state: AgentState) -> dict:
    question = state["question"]
    content, tokens = _llm([
        {"role": "system", "content": _CLASSIFY_SYSTEM},
        {"role": "user", "content": question},
    ])
    try:
        parsed = json.loads(content)
        query_type = parsed.get("type", "simple")
        reason = parsed.get("reason", "")
    except (json.JSONDecodeError, KeyError):
        query_type = "simple"
        reason = "fallback (parse error)"

    return {
        "query_type": query_type,
        "complexity_reason": reason,
        "total_tokens": _merge_tokens(state, tokens),
        "sql_attempts": 0,
        "repair_attempts": 0,
        "semantic_warnings": [],
    }


# ---------------------------------------------------------------------------
# Nó 2: Schema linking
# ---------------------------------------------------------------------------

def make_schema_link_node(schema_store=None, few_shot_store=None):
    """Factory que injeta os índices no nó via closure."""

    def schema_link_node(state: AgentState) -> dict:
        question = state["question"]

        # Schema linking
        if schema_store and schema_store.is_built:
            from src.text2sql.schema_linker import link_schema
            schema_prompt, schema_tables = link_schema(question, schema_store)
        else:
            from src.db.schema import build_schema_prompt, get_schema_info
            tables = get_schema_info()
            schema_prompt = build_schema_prompt(tables)
            schema_tables = [t.name for t in tables]

        # Domain rules
        entries = get_relevant_entries(question)
        domain_rules = format_domain_rules(entries)

        # Few-shot
        if few_shot_store and few_shot_store.is_built:
            from src.text2sql.few_shot_store import get_similar_examples
            examples = get_similar_examples(question, few_shot_store, n=3)
        else:
            from src.text2sql.nl_sql_pairs import get_few_shot_examples
            examples = get_few_shot_examples(question, n=3)
        few_shot = format_few_shot(examples)

        return {
            "schema_prompt": schema_prompt,
            "schema_tables": schema_tables,
            "domain_rules": domain_rules,
            "few_shot": few_shot,
        }

    return schema_link_node


# ---------------------------------------------------------------------------
# Nó 3: Geração de SQL
# ---------------------------------------------------------------------------

def generate_sql_node(state: AgentState) -> dict:
    question = state["question"]
    schema_prompt = state.get("schema_prompt", "")
    domain_rules = state.get("domain_rules", "")
    few_shot = state.get("few_shot", "")
    last_error = state.get("last_error")

    repair_block = ""
    if last_error:
        repair_block = f"\nERRO NA TENTATIVA ANTERIOR (corrija o SQL):\n{last_error}\n"

    user_content = (
        f"SCHEMA DO BANCO:\n{schema_prompt}\n\n"
        f"{domain_rules}\n\n"
        f"EXEMPLOS DE REFERÊNCIA:\n{few_shot}\n"
        f"PERGUNTA: {question}\n"
        f"{repair_block}"
        "SQL:"
    )

    content, tokens = _llm([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ])

    return {
        "sql": content,
        "sql_attempts": (state.get("sql_attempts") or 0) + 1,
        "total_tokens": _merge_tokens(state, tokens),
        "last_error": None,
    }


# ---------------------------------------------------------------------------
# Nó 4: Validação sintática
# ---------------------------------------------------------------------------

def validate_syntax_node(state: AgentState) -> dict:
    sql = state.get("sql", "")
    try:
        sqlglot.parse_one(sql, dialect="duckdb")
        return {"last_error": None}
    except sqlglot.errors.ParseError as e:
        return {"last_error": f"Erro de sintaxe SQL: {e}"}


# ---------------------------------------------------------------------------
# Nó 5: Execução segura
# ---------------------------------------------------------------------------

def execute_node(state: AgentState) -> dict:
    sql = state.get("sql", "")
    sql_upper = sql.upper()

    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"):
        if forbidden in sql_upper:
            return {"last_error": f"Operação '{forbidden}' não permitida."}

    if "LIMIT" not in sql_upper:
        sql = sql.rstrip(";") + f"\nLIMIT {MAX_ROWS};"

    try:
        with get_connection(read_only=True) as conn:
            result = conn.execute(sql)
            rows = result.fetchall()
            columns = [d[0] for d in result.description] if result.description else []

        # Extrai tabelas usadas
        try:
            parsed = sqlglot.parse_one(sql, dialect="duckdb")
            tables = [t.name.lower() for t in parsed.find_all(sqlglot.exp.Table) if t.name]
        except Exception:
            tables = []

        return {
            "rows": [list(r) for r in rows],
            "columns": columns,
            "tables_used": tables,
            "last_error": None,
        }
    except Exception as e:
        return {"last_error": str(e)}


# ---------------------------------------------------------------------------
# Nó 6: Validação semântica do resultado
# ---------------------------------------------------------------------------

_SUSPICIOUS_EMPTY = {
    "quantas", "total", "soma", "custo", "valor", "média", "taxa",
    "quais", "liste", "top", "maior", "menor",
}


def validate_result_node(state: AgentState) -> dict:
    rows = state.get("rows", [])
    columns = state.get("columns", [])
    question = state.get("question", "").lower()
    warnings: list[str] = []
    error: str | None = None

    # 1. Resultado vazio em query que implica dados
    if not rows and any(w in question for w in _SUSPICIOUS_EMPTY):
        error = (
            "A query retornou 0 linhas, mas a pergunta sugere que deveria haver dados. "
            "Verifique os filtros ou joins."
        )
        return {"last_error": error, "semantic_warnings": warnings}

    # 2. Taxas e percentuais impossíveis (> 100%)
    rate_keywords = ("taxa", "percentual", "pct", "percent", "rate")
    for i, col in enumerate(columns):
        col_lower = col.lower()
        if any(k in col_lower for k in rate_keywords):
            for row in rows:
                if i < len(row) and isinstance(row[i], (int, float)) and row[i] > 100:
                    warnings.append(
                        f"Coluna '{col}' tem valor {row[i]:.2f} > 100 — taxa suspeita."
                    )

    # 3. Custo por internação absurdo (> R$ 500k)
    cost_keywords = ("custo", "valor", "val_tot", "cost")
    for i, col in enumerate(columns):
        if any(k in col.lower() for k in cost_keywords):
            for row in rows:
                if i < len(row) and isinstance(row[i], (int, float)) and row[i] > 500_000:
                    warnings.append(
                        f"Coluna '{col}' tem valor R$ {row[i]:,.2f} — custo unitário suspeito."
                    )

    # 4. Ano fora do escopo 2008-2023
    year_keywords = ("ano",)
    for i, col in enumerate(columns):
        if any(k in col.lower() for k in year_keywords):
            for row in rows:
                if i < len(row) and isinstance(row[i], (int, float)):
                    if row[i] < 2008 or row[i] > 2023:
                        warnings.append(
                            f"Ano {int(row[i])} fora do escopo do banco (2008–2023)."
                        )

    return {"semantic_warnings": warnings, "last_error": None}


# ---------------------------------------------------------------------------
# Nó 7: Repair
# ---------------------------------------------------------------------------

def repair_node(state: AgentState) -> dict:
    """Incrementa contador de repairs. O erro já está em last_error."""
    return {
        "repair_attempts": (state.get("repair_attempts") or 0) + 1,
    }


# ---------------------------------------------------------------------------
# Nó 8: Decomposição (path complexo)
# ---------------------------------------------------------------------------

_DECOMPOSE_SYSTEM = """Você é um assistente que decompõe perguntas complexas em sub-perguntas
independentes para o banco SIH-RD DATASUS (internações RS + MA, 2008-2023).

Decomponha a pergunta em 2 a 3 sub-perguntas simples que:
  - Cada uma pode ser respondida com UMA query SQL
  - Juntas, permitem responder a pergunta original

Responda APENAS com JSON válido:
{
  "sub_questions": ["sub-pergunta 1", "sub-pergunta 2", ...],
  "aggregation_note": "como combinar os resultados para responder a pergunta original"
}"""


def make_decompose_node(schema_store=None, few_shot_store=None):
    """Factory que injeta stores no nó de decomposição."""

    def decompose_node(state: AgentState) -> dict:
        question = state["question"]

        # LLM decompõe a pergunta
        content, tokens = _llm([
            {"role": "system", "content": _DECOMPOSE_SYSTEM},
            {"role": "user", "content": question},
        ])

        try:
            parsed = json.loads(content)
            sub_questions = parsed.get("sub_questions", [question])
            aggregation_note = parsed.get("aggregation_note", "")
        except (json.JSONDecodeError, KeyError):
            sub_questions = [question]
            aggregation_note = "Responda com base no resultado disponível."

        # Executa cada sub-query via pipeline Fase 2 (simples, sem decomposição)
        from src.text2sql.pipeline import Text2SQLPipeline
        sub_pipeline = Text2SQLPipeline(
            schema_store=schema_store,
            few_shot_store=few_shot_store,
        )

        sub_results: list[dict[str, Any]] = []
        for sq in sub_questions:
            r = sub_pipeline.run(sq)
            sub_results.append({
                "question": sq,
                "sql": r.sql,
                "columns": r.columns,
                "rows": [list(row) for row in r.rows],
                "success": r.success,
                "error": r.error,
            })

        return {
            "sub_questions": sub_questions,
            "sub_results": sub_results,
            "total_tokens": _merge_tokens(state, tokens),
            # Guarda aggregation_note no domain_rules para o explainer
            "domain_rules": f"Nota de agregação: {aggregation_note}",
        }

    return decompose_node


# ---------------------------------------------------------------------------
# Nó 9: Explainer — resposta em linguagem natural
# ---------------------------------------------------------------------------

_EXPLAIN_SYSTEM = """Você é um analista de dados especializado em saúde pública brasileira.
Com base na pergunta, no SQL executado e nos resultados obtidos, produza uma resposta
clara e direta em português.

Diretrizes:
- 2 a 4 frases objetivas
- Mencione os números mais relevantes do resultado
- Se houver avisos semânticos, mencione brevemente
- Contextualize quando necessário (ex: "8,25% é uma taxa elevada para internações clínicas")
- Se o resultado vier de múltiplas sub-queries, integre os achados numa narrativa coerente
- NÃO inclua o SQL na resposta final"""


def _format_result_for_llm(columns: list[str], rows: list[list], max_rows: int = 15) -> str:
    if not rows:
        return "(resultado vazio)"
    header = " | ".join(columns)
    sep = "-" * len(header)
    lines = [header, sep]
    for row in rows[:max_rows]:
        lines.append(" | ".join(str(v) for v in row))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} linhas omitidas)")
    return "\n".join(lines)


def explain_node(state: AgentState) -> dict:
    question = state["question"]
    query_type = state.get("query_type", "simple")
    warnings = state.get("semantic_warnings", [])

    # Monta contexto de resultado
    if query_type == "complex":
        sub_results = state.get("sub_results", [])
        aggregation_note = state.get("domain_rules", "")
        results_text = ""
        for sr in sub_results:
            results_text += f"\nSub-pergunta: {sr['question']}\n"
            if sr["success"]:
                results_text += _format_result_for_llm(sr["columns"], sr["rows"])
            else:
                results_text += f"Erro: {sr['error']}"
            results_text += "\n"
        context = f"Nota de combinação: {aggregation_note}\n\nResultados:\n{results_text}"
    else:
        sql = state.get("sql", "")
        columns = state.get("columns", [])
        rows = state.get("rows", [])
        result_table = _format_result_for_llm(columns, rows)
        context = f"SQL executado:\n{sql}\n\nResultado:\n{result_table}"

    warning_text = ""
    if warnings:
        warning_text = f"\nAvisos semânticos: {'; '.join(warnings)}"

    user_content = (
        f"Pergunta: {question}\n\n"
        f"{context}"
        f"{warning_text}"
    )

    content, tokens = _llm(
        [
            {"role": "system", "content": _EXPLAIN_SYSTEM},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,  # ligeiramente criativo para texto natural
    )

    return {
        "answer": content,
        "success": True,
        "error": None,
        "total_tokens": _merge_tokens(state, tokens),
    }
