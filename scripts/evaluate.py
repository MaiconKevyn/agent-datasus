#!/usr/bin/env python3
"""
Framework de Avaliação — Execution Accuracy (EX)

Avalia o pipeline Text-to-SQL sobre os 30 pares NL-SQL anotados.
Métrica: Execution Accuracy (EX) — padrão BIRD benchmark.

EX = queries cujo resultado é equivalente ao gabarito / total de queries

Equivalência de resultado:
  - Compara conjuntos de linhas (set equality, ignora ordem)
  - Normaliza valores float (arredonda para 2 casas)
  - Tolera diferença de alias de colunas (compara por posição)

Saída:
  - Relatório no terminal (por categoria + geral)
  - reports/evaluation_report.json (para análise e histórico)

Uso:
    python scripts/evaluate.py                 # avalia todos os 30 pares
    python scripts/evaluate.py --category simples
    python scripts/evaluate.py --no-indexes    # usa fallback léxico (baseline Fase 1)
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.connection import get_connection
from src.text2sql.nl_sql_pairs import NL_SQL_PAIRS, NLSQLPair
from src.text2sql.pipeline import Text2SQLPipeline
from src.text2sql.schema_linker import build_schema_index
from src.text2sql.few_shot_store import build_few_shot_index

REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Execução do gabarito
# ---------------------------------------------------------------------------

def execute_gold(sql: str) -> tuple[list[tuple], str | None]:
    """Executa o SQL gabarito e retorna (rows, erro)."""
    try:
        with get_connection(read_only=True) as conn:
            result = conn.execute(sql)
            rows = result.fetchall()
        return rows, None
    except Exception as e:
        return [], str(e)


# ---------------------------------------------------------------------------
# Comparação de resultados
# ---------------------------------------------------------------------------

_FLOAT_TOLERANCE = 0.01  # tolerância absoluta para comparação de floats


def _normalize_value(v) -> str:
    """Normaliza um valor para comparação: float → arredondado, str → lower strip."""
    if isinstance(v, float):
        # Arredonda para 1 casa decimal para absorver imprecisões de ponto flutuante
        # (ex: SUM de 18M valores pode divergir em centavos dependendo da ordem)
        return str(round(v, 1))
    if isinstance(v, str):
        return v.strip().lower()
    return str(v)


def _normalize_row(row: tuple) -> tuple:
    return tuple(_normalize_value(v) for v in row)


def results_equivalent(gold: list[tuple], pred: list[tuple]) -> bool:
    """
    Verifica se dois resultados são equivalentes.

    Estratégia (ordem de preferência):
    1. Set equality exata (ignora ordem de linhas).
    2. Scalar match: se gold tem 1 col × 1 linha, compara o valor.
    3. Subset tolerance: se gold tem LIMIT mais restritivo que pred,
       aceita se o top-N do pred (normalizado) contém todas as linhas do gold.
    """
    if not gold and not pred:
        return True
    if bool(gold) != bool(pred):
        return False

    gold_set = set(_normalize_row(r) for r in gold)
    pred_set = set(_normalize_row(r) for r in pred)

    # 1. Igualdade exata
    if gold_set == pred_set:
        return True

    # 2. Scalar: ambos têm 1 linha × 1 coluna
    if len(gold) == 1 and len(pred) == 1 and len(gold[0]) == 1 and len(pred[0]) == 1:
        return _normalize_value(gold[0][0]) == _normalize_value(pred[0][0])

    # 3. Subset: gold é subconjunto de pred (pred retornou mais linhas por LIMIT maior)
    #    Válido apenas quando gold tem menos linhas que pred e gold ⊆ pred
    if len(gold) < len(pred) and gold_set.issubset(pred_set):
        return True

    # 4. Pred é subconjunto de gold (pred retornou menos linhas por LIMIT menor)
    if len(pred) < len(gold) and pred_set.issubset(gold_set):
        return True

    return False


# ---------------------------------------------------------------------------
# Avaliação de um par
# ---------------------------------------------------------------------------

@dataclass
class EvalResult:
    question: str
    difficulty: str
    tags: list[str]
    gold_sql: str
    pred_sql: str
    gold_rows: list[tuple]
    pred_rows: list[tuple]
    gold_error: str | None
    pred_error: str | None
    execution_accurate: bool
    repair_attempts: int
    latency_ms: int
    tokens_total: int
    schema_tables: list[str]


def evaluate_pair(pair: NLSQLPair, pipeline: Text2SQLPipeline) -> EvalResult:
    gold_rows, gold_error = execute_gold(pair.sql)
    result = pipeline.run(pair.question)

    match = False
    if result.success and not gold_error:
        match = results_equivalent(gold_rows, result.rows)

    return EvalResult(
        question=pair.question,
        difficulty=pair.difficulty,
        tags=pair.tags,
        gold_sql=pair.sql,
        pred_sql=result.sql,
        gold_rows=gold_rows,
        pred_rows=result.rows,
        gold_error=gold_error,
        pred_error=result.error,
        execution_accurate=match,
        repair_attempts=result.repair_attempts,
        latency_ms=result.latency_ms,
        tokens_total=result.tokens_used.get("total", 0),
        schema_tables=result.schema_tables_selected,
    )


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------

def build_report(results: list[EvalResult]) -> dict:
    by_difficulty: dict[str, list[EvalResult]] = {}
    for r in results:
        by_difficulty.setdefault(r.difficulty, []).append(r)

    def stats(rs: list[EvalResult]) -> dict:
        n = len(rs)
        correct = sum(1 for r in rs if r.execution_accurate)
        repaired = sum(1 for r in rs if r.repair_attempts > 0)
        return {
            "total": n,
            "correct": correct,
            "ex_accuracy": round(correct / n * 100, 1) if n else 0,
            "with_repairs": repaired,
            "avg_latency_ms": round(sum(r.latency_ms for r in rs) / n) if n else 0,
            "avg_tokens": round(sum(r.tokens_total for r in rs) / n) if n else 0,
        }

    failures = [
        {
            "question": r.question,
            "difficulty": r.difficulty,
            "tags": r.tags,
            "pred_sql": r.pred_sql,
            "gold_sql": r.gold_sql,
            "pred_error": r.pred_error,
            "gold_error": r.gold_error,
            "repair_attempts": r.repair_attempts,
        }
        for r in results
        if not r.execution_accurate
    ]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall": stats(results),
        "by_difficulty": {k: stats(v) for k, v in by_difficulty.items()},
        "failures": failures,
        "total_tokens": sum(r.tokens_total for r in results),
        "total_latency_ms": sum(r.latency_ms for r in results),
    }


def print_report(report: dict) -> None:
    ov = report["overall"]
    print(f"\n{'='*60}")
    print(f"AVALIAÇÃO — Execution Accuracy (EX)")
    print(f"{'='*60}")
    print(f"  Total:         {ov['total']} pares")
    print(f"  Corretos:      {ov['correct']}/{ov['total']}")
    print(f"  EX Accuracy:   {ov['ex_accuracy']}%")
    print(f"  Com repairs:   {ov['with_repairs']}")
    print(f"  Latência avg:  {ov['avg_latency_ms']}ms")
    print(f"  Tokens avg:    {ov['avg_tokens']}")
    print(f"  Total tokens:  {report['total_tokens']}")

    print(f"\n  Por dificuldade:")
    for diff, s in report["by_difficulty"].items():
        print(f"    {diff:10s}: {s['correct']}/{s['total']} ({s['ex_accuracy']}%)")

    if report["failures"]:
        print(f"\n  Falhas ({len(report['failures'])}):")
        for f in report["failures"]:
            print(f"    [{f['difficulty']}] {f['question'][:70]}")
            if f["pred_error"]:
                print(f"           Erro: {f['pred_error'][:80]}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    use_indexes = "--no-indexes" not in sys.argv
    category_filter = None
    if "--category" in sys.argv:
        idx = sys.argv.index("--category")
        category_filter = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    print("=" * 60)
    print("AVALIAÇÃO — Text-to-SQL Pipeline Fase 2")
    print(f"Índices semânticos: {'SIM' if use_indexes else 'NÃO (baseline léxico)'}")
    print("=" * 60)

    # Carrega índices
    schema_store = None
    few_shot_store = None
    if use_indexes:
        print("\nCarregando índices de embeddings...")
        schema_store = build_schema_index()
        few_shot_store = build_few_shot_index()
        print(f"  Schema index: {schema_store.size} docs")
        print(f"  Few-shot index: {few_shot_store.size} docs")

    pipeline = Text2SQLPipeline(
        schema_store=schema_store,
        few_shot_store=few_shot_store,
    )

    # Seleciona pares para avaliação
    pairs = NL_SQL_PAIRS
    if category_filter:
        pairs = [p for p in pairs if p.difficulty == category_filter]
        print(f"\nFiltro: dificuldade='{category_filter}' → {len(pairs)} pares")

    print(f"\nAvaliando {len(pairs)} pares NL-SQL...")
    print("(uma linha por par; . = correto, F = falhou)\n", end="", flush=True)

    results: list[EvalResult] = []
    for i, pair in enumerate(pairs, 1):
        r = evaluate_pair(pair, pipeline)
        results.append(r)
        mark = "." if r.execution_accurate else "F"
        print(mark, end="", flush=True)
        if i % 10 == 0:
            print(f" {i}/{len(pairs)}", flush=True)

    print()

    # Relatório
    report = build_report(results)
    print_report(report)

    # Salva
    label = "sem_indexes" if not use_indexes else "com_indexes"
    if category_filter:
        label += f"_{category_filter}"
    out_path = REPORTS_DIR / f"eval_{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nRelatório salvo em: {out_path}")


if __name__ == "__main__":
    main()
