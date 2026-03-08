#!/usr/bin/env python3
"""
Demo do agente multiagente (Fase 4).

Executa perguntas simples e complexas mostrando:
  - Tipo de query classificado
  - SQL gerado (ou sub-queries decompostas)
  - Resposta em linguagem natural do Explainer
  - Avisos semânticos, repairs, tokens

Uso:
    python scripts/demo_agent.py
    python scripts/demo_agent.py "sua pergunta aqui"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.agent.graph import build_graph, run_agent
from src.text2sql.few_shot_store import build_few_shot_index
from src.text2sql.schema_linker import build_schema_index

DEMO_QUESTIONS = [
    # Simples — path direto
    "Quantas internações ocorreram em 2022?",
    "Quais os 5 diagnósticos mais frequentes no Maranhão?",
    # Médio — simples mas com join complexo
    "Qual a taxa de mortalidade por especialidade, ordenada da maior para a menor?",
    # Complexo — requer múltiplas queries e síntese
    "Compare a mortalidade hospitalar e o custo médio entre RS e MA, "
    "explicando qual estado tem melhor eficiência assistencial",
    # Complexo — análise cruzada com socioeconômico
    "Qual a diferença na taxa de mortalidade entre municípios com IDH alto e baixo? "
    "Considere apenas municípios com pelo menos 500 internações.",
]


def print_result(result) -> None:
    sep = "=" * 70
    print(f"\n{sep}")
    icon = "✅" if result.success else "❌"
    print(f"{icon} [{result.query_type.upper()}] {result.question}")
    print(f"   Motivo: {result.complexity_reason}")
    print(f"   Latência: {result.latency_ms}ms | Tokens: {result.total_tokens.get('total', 0)} | "
          f"Repairs: {result.repair_attempts}")

    if result.query_type == "complex" and result.sub_questions:
        print(f"\n   Sub-queries ({len(result.sub_questions)}):")
        for i, sq in enumerate(result.sub_questions, 1):
            sr = result.sub_results[i - 1] if i - 1 < len(result.sub_results) else {}
            status = "OK" if sr.get("success") else "ERRO"
            print(f"     {i}. [{status}] {sq}")
            if sr.get("sql"):
                sql_preview = sr["sql"].strip().split("\n")[0]
                print(f"        SQL: {sql_preview}...")
            if sr.get("rows"):
                print(f"        Resultado: {sr['rows'][:2]}{'...' if len(sr['rows']) > 2 else ''}")
    elif result.sql:
        sql_display = result.sql.strip().replace("\n", "\n   ")
        print(f"\n   SQL:\n   {sql_display}")
        if result.rows:
            print(f"\n   Resultado ({len(result.rows)} linhas):")
            for row in result.rows[:4]:
                print(f"     {row}")
            if len(result.rows) > 4:
                print(f"     ... ({len(result.rows) - 4} mais)")

    if result.semantic_warnings:
        print(f"\n   ⚠️  Avisos: {'; '.join(result.semantic_warnings)}")

    if result.answer:
        print(f"\n   💬 Resposta:\n   {result.answer}")

    if not result.success and result.error:
        print(f"\n   Erro: {result.error}")


def main():
    print("=" * 70)
    print("DEMO — Text-to-SQL Multiagente (Fase 4 — LangGraph)")
    print("Orchestrator → Classifier → [Decomposer | SQL Agent] → Validator → Explainer")
    print("=" * 70)

    print("\nCarregando índices...")
    schema_store = build_schema_index()
    few_shot_store = build_few_shot_index()
    graph = build_graph(schema_store, few_shot_store)
    print(f"Pronto. Schema: {schema_store.size} docs | Few-shot: {few_shot_store.size} docs\n")

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    questions = [" ".join(args)] if args else DEMO_QUESTIONS

    for question in questions:
        result = run_agent(question, compiled_graph=graph)
        print_result(result)

    print(f"\n{'=' * 70}")


if __name__ == "__main__":
    main()
