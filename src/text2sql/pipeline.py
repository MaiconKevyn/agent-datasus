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

# ---------------------------------------------------------------------------
# Prompt do planejador (CoT antes da geração de SQL)
# ---------------------------------------------------------------------------

PLAN_SYSTEM_PROMPT = """Você é um planejador de queries SQL para o banco SIH-RD do DATASUS.
Dado um schema e uma pergunta, produza um plano estruturado ANTES de gerar o SQL.

Responda com JSON válido no seguinte formato:
{
  "tabelas": ["internacoes", "cid", "municipios", ...],
  "filtros": "filtros WHERE a aplicar (MORTE=TRUE, IDADE BETWEEN x AND y, etc.)",
  "agrupamento": "colunas para GROUP BY",
  "top_n_por_grupo": {
    "ativo": true | false,
    "n": 5,
    "grupo": "coluna de partição (estado, faixa_etaria, sexo, ...)",
    "ordenar_por": "métrica para ORDER BY dentro de cada grupo"
  },
  "escopo_geografico": "sem filtro | WHERE estado='RS' | WHERE estado IN ('MA','RS') | etc.",
  "metrica": "o que calcular (COUNT(*), AVG(VAL_UTI), SUM(MORTE::INT), etc.)",
  "colunas_saida": ["col1", "col2", ...],
  "join_cid": true | false,
  "notas": "observações especiais"
}

REGRA para join_cid:
- Se a pergunta menciona "diagnóstico", "motivo de internação", "causa de morte", "CID", "doença" →
  join_cid=true e use cid.CD_DESCRICAO como coluna de saída, não DIAG_PRINC (que é código, não nome)
- JOIN: internacoes.DIAG_PRINC = cid.CID (ou CID_MORTE = cid.CID para causa de morte)
- NUNCA exiba o código bruto DIAG_PRINC na saída quando a pergunta quer nomes de diagnósticos

REGRAS para top_n_por_grupo:
- Se a pergunta pede "top N por [grupo]", "N mais X para cada [grupo]", "N de cada [grupo]" → ativo=true
- Exemplos: "3 hospitais por estado", "10 diagnósticos por faixa etária", "5 procedimentos para cada sexo"
- Quando ativo=true, a SQL DEVE usar ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY metrica DESC) e filtrar rn <= N
- NUNCA use apenas LIMIT N quando o resultado esperado é N por grupo (total = N × num_grupos)

REGRA para escopo_geografico:
- Pergunta menciona RS/MA explicitamente como agrupamento → WHERE estado IN ('MA','RS')
- Pergunta menciona um estado específico para filtro → WHERE estado = 'RS' (ou 'MA')
- Pergunta não menciona estado → sem filtro geográfico"""

SYSTEM_PROMPT = """Você é um especialista em SQL para o banco SIH-RD do DATASUS.
O banco contém dados de internações hospitalares do SUS nos estados RS e MA, período 2008-2023.
Use EXCLUSIVAMENTE DuckDB SQL.

REGRAS CRÍTICAS — NUNCA IGNORE:

1. SEXO: Masculino=1, Feminino=3 (não existe valor 2).

2. MORTE é BOOLEAN. Para filtrar óbitos: MORTE=TRUE. Para somar: SUM(MORTE::INT).

3. Para CONTAR internações: sempre use a tabela 'internacoes', nunca 'atendimentos'.

4. CUSTOS — use a coluna correta conforme o que é pedido:
   - Custo TOTAL da internação       → VAL_TOT
   - Custo do serviço HOSPITALAR     → VAL_SH   (nunca confunda com VAL_TOT)
   - Custo do serviço PROFISSIONAL   → VAL_SP
   - Custo/valor de UTI              → VAL_UTI
   "Valor total" ou "custo total" sem especificação → VAL_TOT.
   "Serviço hospitalar" → VAL_SH. "Serviço profissional" → VAL_SP.

5. UTI — para identificar internações em UTI use SEMPRE VAL_UTI > 0.
   NÃO use UTI_INT_TO para filtrar. UTI_INT_TO conta dias em UTI (duração), VAL_UTI é o custo.
   - ERRADO: WHERE UTI_INT_TO > 0  (para identificar se houve UTI)
   - CERTO:  WHERE VAL_UTI > 0

6. ESPECIALIDADE (ESPEC) — códigos obrigatórios:
   1 = Cirúrgica    2 = Obstétrica    3 = Clínica médica
   4 = Hospital-dia 5 = Psiquiatria   7 = Pediátrica
   "Obstétrico/obstetrícia/parto" → ESPEC = 2
   "Psiquiatria/psiquiátrico"     → ESPEC = 5
   "Cirurgia/cirúrgico"           → ESPEC = 1

7. Para município do paciente: MUNIC_RES → municipios.codigo_6d.
   Para município do hospital:  hospital.MUNIC_MOV → municipios.codigo_6d.
   Quando a pergunta pede "municípios com mais internações/pacientes" SEM especificar hospital ou paciente,
   use MUNIC_RES como padrão (perspectiva do paciente).
   Quando a pergunta menciona "cidades/municípios que ATENDEM" ou "onde fica o hospital", use MUNIC_MOV.
   Quando exibir município na saída: SEMPRE mostre municipios.nome (nunca retorne o código numérico MUNIC_RES ou MUNIC_MOV).

8. FILTRO GEOGRÁFICO — duas regras opostas, ambas obrigatórias:
   A) Se a pergunta NÃO menciona estado/RS/MA/Rio Grande do Sul/Maranhão → NÃO adicione WHERE estado. Ponto final.
      ERRADO: SELECT MAX(VAL_TOT) FROM internacoes JOIN municipios ... WHERE estado='RS'  ← não pedido
      CERTO:  SELECT MAX(VAL_TOT) FROM internacoes
      ERRADO: SELECT h.CNES, COUNT(*) FROM internacoes JOIN hospital h ... WHERE m.estado='RS'  ← não pedido
      CERTO:  SELECT h.CNES, COUNT(*) FROM internacoes JOIN hospital h ... GROUP BY h.CNES
   B) Se a pergunta menciona "MA e RS", "em cada estado", "por estado (MA e RS)" → adicione WHERE mu.estado IN ('MA', 'RS').
      Isso é necessário porque MUNIC_RES pode conter pacientes de outros estados (AC, SP, etc.) internados em RS/MA.
      ERRADO: GROUP BY estado sem filtro → retorna AC, SP, MG, etc.
      CERTO:  JOIN municipios mu ON ... WHERE mu.estado IN ('MA', 'RS') GROUP BY mu.estado
   AUTO-CHECK: Pergunta menciona RS/MA? Se NÃO → sem WHERE estado. Se SIM (como agrupamento) → WHERE estado IN ('MA','RS').

9. TABELA CID — colunas DIAG_PRINC, DIAG_SECUN e CID_MORTE contêm CÓDIGOS CID (ex: 'A15', 'J00').
   NUNCA use ILIKE nessas colunas — elas não contêm nomes de doenças.
   Para filtrar por NOME de doença: faça JOIN com cid e use cid.CD_DESCRICAO ILIKE '%meningite%'.
     ERRADO: WHERE DIAG_PRINC ILIKE '%meningite%'  ← DIAG_PRINC é código, não nome!
     CERTO:  JOIN cid ON internacoes.DIAG_PRINC = cid.CID WHERE cid.CD_DESCRICAO ILIKE '%meningite%'
   Para filtrar por CATEGORIA CID (prefixo): use LIKE 'J%' (doenças respiratórias = J00-J99).
     ERRADO: WHERE DIAG_PRINC IN ('J00', 'J01', 'J02', ...)  ← nunca enumere manualmente
     CERTO:  WHERE DIAG_PRINC LIKE 'J%'
   Coluna de descrição: cid.CD_DESCRICAO (NÃO existe cid.DESCRICAO).
   JOIN: internacoes.DIAG_PRINC = cid.CID (ou CID_MORTE = cid.CID).
   Quando a pergunta pede "diagnóstico mais comum", "motivo de internação", "causa de morte"
   → sempre faça JOIN com cid e retorne cid.CD_DESCRICAO, agrupando por CD_DESCRICAO.

10. PACIENTES INDÍGENAS → use RACA_COR = 5 (NÃO use a coluna ETNIA para isso).
    A coluna ETNIA é específica para subgrupos étnicos indígenas, não para identificar indígenas.
    Valores RACA_COR: 1=Branca, 2=Preta, 3=Parda, 4=Amarela, 5=Indígena, 99=Sem informação.

11. SEXO na saída: quando o resultado mostrar sexo do paciente, exiba o rótulo textual:
    CASE WHEN SEXO=1 THEN 'Masculino' WHEN SEXO=3 THEN 'Feminino' ELSE 'Outro' END
    Nunca retorne apenas o código numérico 1 ou 3 como valor de sexo na saída.

12. 'socioeconomico' é formato long: sempre filtre por metrica (ex: metrica = 'idhm').

12b. VINCPREV (vínculo previdenciário): VINCPREV=0 significa "sem vínculo informado / não informado".
     Para contar pacientes COM vínculo informado: WHERE VINCPREV IS NOT NULL AND VINCPREV != 0
     Para contar pacientes SEM vínculo: WHERE VINCPREV IS NULL OR VINCPREV = 0

13. TOP-N por grupo/estado/especialidade: use ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY ...) <= N.
    PALAVRAS-CHAVE que obrigam PARTITION BY: "em cada estado", "por estado", "de cada ano",
    "por ano", "para cada faixa etária", "por faixa etária", "para cada sexo", "por especialidade",
    "em cada mês", "por hospital", "para cada grupo".
    ERRADO: GROUP BY estado, cid ORDER BY total DESC LIMIT 3  ← retorna 3 globais
    CERTO:  ROW_NUMBER() OVER (PARTITION BY estado ORDER BY total DESC) AS rn ... WHERE rn <= 3
    Exemplo: "top 3 diagnósticos POR estado" → PARTITION BY estado; "top 1 por ano" → PARTITION BY ano.

13. Se a pergunta pedir "top N" sem especificar agrupamento, use LIMIT N. Caso contrário, LIMIT 500.

14. Retorne APENAS o SQL, sem explicações, sem markdown, sem comentários.

15. Use alias descritivos nas colunas (AS nome_coluna).
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
    plan: str | None = None


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

class Text2SQLPipeline:
    def __init__(
        self,
        schema_store: EmbeddingStore | None = None,
        few_shot_store: FewShotStore | None = None,
        use_planning: bool = False,
    ) -> None:
        """
        Args:
            schema_store: Índice de schema pré-construído (EmbeddingStore).
                          Se None, usa schema completo como fallback.
            few_shot_store: Índice de pares NL-SQL pré-construído.
                            Se None, usa matching léxico como fallback.
            use_planning: Se True, gera um plano CoT antes de gerar SQL.
                          O plano é incluído no prompt de geração para guiar o LLM.
        """
        self._client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self._schema_store = schema_store
        self._few_shot_store = few_shot_store
        self._use_planning = use_planning
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

    # ── Etapa 1b: Geração de plano CoT (opcional) ────────────────────────────

    def _generate_plan(
        self, question: str, schema_prompt: str
    ) -> tuple[str, dict]:
        """
        Chama o LLM para gerar um plano estruturado (JSON) antes da geração de SQL.
        Retorna (plan_text, tokens).
        """
        import json as _json
        user_content = (
            f"SCHEMA DO BANCO:\n{schema_prompt}\n\n"
            f"PERGUNTA: {question}\n\n"
            "Gere o plano JSON:"
        )
        response = self._client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": PLAN_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        plan_text = response.choices[0].message.content.strip()
        tokens = {
            "input": response.usage.prompt_tokens,
            "output": response.usage.completion_tokens,
            "total": response.usage.total_tokens,
        }
        # Tenta formatar o JSON para inclusão legível no prompt
        try:
            parsed = _json.loads(plan_text)
            plan_text = _json.dumps(parsed, ensure_ascii=False, indent=2)
        except Exception:
            pass
        return plan_text, tokens

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
        plan: str | None = None,
    ) -> list[dict]:
        import json as _json

        repair_block = ""
        if error:
            repair_block = f"\nERRO NA TENTATIVA ANTERIOR (corrija o SQL):\n{error}\n"

        plan_block = ""
        if plan:
            # Extrai apenas os avisos específicos do plano — NÃO inclui o JSON completo
            # (incluir o plano completo introduz erros do planejador que causam regressões)
            warnings_list: list[str] = []
            try:
                parsed_plan = _json.loads(plan)

                # Detecta top_n_por_grupo → injeta template CTE obrigatório
                top_n = parsed_plan.get("top_n_por_grupo", {})
                if top_n.get("ativo") is True:
                    n = top_n.get("n", "N")
                    grupo = top_n.get("grupo", "grupo")
                    ordenar = top_n.get("ordenar_por", "COUNT(*) DESC")
                    warnings_list.append(
                        f"⚠️  TOP-N POR GRUPO detectado — use OBRIGATORIAMENTE o padrão CTE:\n"
                        f"WITH ranked AS (\n"
                        f"    SELECT {grupo} AS grupo_col, <colunas_detalhe>, <metrica>,\n"
                        f"           ROW_NUMBER() OVER (PARTITION BY {grupo} ORDER BY {ordenar}) AS rn\n"
                        f"    FROM <tabelas> WHERE <filtros>\n"
                        f"    GROUP BY {grupo}, <colunas_detalhe>\n"
                        f")\n"
                        f"SELECT grupo_col, <colunas_detalhe>, <metrica> FROM ranked WHERE rn <= {n};\n"
                        f"NUNCA use HAVING ROW_NUMBER() (causa erro DuckDB). "
                        f"Total esperado: {n} × nº grupos."
                    )

                # Detecta join_cid → injeta instrução de JOIN cid
                if parsed_plan.get("join_cid") is True:
                    warnings_list.append(
                        "⚠️  JOIN CID NECESSÁRIO — use cid.CD_DESCRICAO:\n"
                        "   JOIN cid ON internacoes.DIAG_PRINC = cid.CID\n"
                        "   NUNCA retorne código bruto DIAG_PRINC — retorne cid.CD_DESCRICAO."
                    )
            except Exception:
                pass

            if warnings_list:
                plan_block = (
                    "\nINSTRUÇÕES ADICIONAIS (baseadas na análise da pergunta):\n"
                    + "\n\n".join(warnings_list) + "\n"
                )

        user_content = (
            f"SCHEMA DO BANCO:\n{schema_prompt}\n\n"
            f"{domain_rules}\n\n"
            f"EXEMPLOS DE REFERÊNCIA:\n{few_shot}\n"
            f"PERGUNTA: {question}\n"
            f"{plan_block}"
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

            # Etapa 1b: planejamento CoT (opcional)
            plan: str | None = None
            if self._use_planning:
                try:
                    plan, plan_tokens = self._generate_plan(question, schema_prompt)
                    for k in total_tokens:
                        total_tokens[k] += plan_tokens.get(k, 0)
                    trace.log_span("planning", plan_preview=plan[:200])
                except Exception as e:
                    trace.log_span("planning_error", error=str(e))

            sql = ""
            error: str | None = None
            repair_attempts = 0

            for attempt in range(MAX_REPAIR_ATTEMPTS + 1):

                # Etapa 3+4: monta prompt e gera SQL
                messages = self._build_prompt(
                    question, schema_prompt, domain_rules, few_shot,
                    error if attempt > 0 else None,
                    plan=plan,
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
                    plan=plan,
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
