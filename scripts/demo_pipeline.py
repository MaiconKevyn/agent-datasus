#!/usr/bin/env python3
"""
Demonstração interativa do pipeline Text-to-SQL.

Uso:
    python scripts/demo_pipeline.py                        # roda 4 perguntas de demo
    python scripts/demo_pipeline.py "sua pergunta aqui"    # pergunta customizada
    python scripts/demo_pipeline.py --no-indexes           # força fallback léxico

Requer índices construídos: python scripts/build_indexes.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.text2sql.pipeline import Text2SQLPipeline
from src.text2sql.logger import TraceLogger

DEMO_QUESTIONS = [
    "Quantas internações ocorreram em 2022?",
    "Quais os 5 diagnósticos mais frequentes no Maranhão?",
    "Qual a taxa de mortalidade hospitalar por especialidade?",
    "Compare o custo médio por internação entre RS e MA ao longo dos anos",
]


def print_result(result) -> None:
    status = "OK" if result.success else "ERRO"
    print(f"\n[{status}] {result.question}")
    print(f"  Latência:  {result.latency_ms}ms | Tokens: {result.tokens_used} | Repairs: {result.repair_attempts}")
    print(f"  Schema:    {result.schema_tables_selected}")
    print(f"  Tabelas:   {result.tables_used}")
    sql_display = result.sql.strip().replace("\n", "\n    ")
    print(f"  SQL:\n    {sql_display}")

    if result.success:
        print(f"  Resultado ({len(result.rows)} linhas):")
        if result.columns:
            print(f"    {' | '.join(result.columns)}")
            print(f"    {'-' * 60}")
        for row in result.rows[:5]:
            print(f"    {row}")
        if len(result.rows) > 5:
            print(f"    ... ({len(result.rows) - 5} linhas omitidas)")
    else:
        print(f"  ERRO: {result.error}")


def main():
    use_indexes = "--no-indexes" not in sys.argv

    print("=" * 70)
    print("DEMO — Text-to-SQL sobre SIH-RD DATASUS (Fase 2)")
    print(f"Modelo: gpt-4o-mini | Schema linking: {'semântico' if use_indexes else 'léxico (fallback)'}")
    print("=" * 70)

    # Carrega índices se disponíveis
    schema_store = None
    few_shot_store = None
    if use_indexes:
        from src.text2sql.schema_linker import build_schema_index
        from src.text2sql.few_shot_store import build_few_shot_index
        schema_store = build_schema_index()
        few_shot_store = build_few_shot_index()
        if not schema_store.is_built:
            print("AVISO: índices não encontrados. Execute: python scripts/build_indexes.py")
            print("       Usando fallback léxico.\n")
            schema_store = None
            few_shot_store = None

    pipeline = Text2SQLPipeline(
        schema_store=schema_store,
        few_shot_store=few_shot_store,
    )

    # Pergunta customizada ou demo
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    questions = [" ".join(args)] if args else DEMO_QUESTIONS

    for question in questions:
        result = pipeline.run(question)
        print_result(result)
        print()

    # Resumo dos traces
    logger = TraceLogger()
    summary = logger.summary()
    print(f"\n--- Resumo de traces (total histórico) ---")
    print(f"  Total execuções: {summary.get('total', 0)}")
    print(f"  Taxa de sucesso: {summary.get('success_rate', 0)}%")
    print(f"  Latência média:  {summary.get('avg_latency_ms', 0)}ms")
    print(f"  Tokens médios:   {summary.get('avg_tokens_total', 0)}")


if __name__ == "__main__":
    main()
