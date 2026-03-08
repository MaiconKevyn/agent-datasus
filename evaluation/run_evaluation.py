"""
run_evaluation.py — CLI entry point para o framework de avaliação.

Uso:
    python evaluation/run_evaluation.py
    python evaluation/run_evaluation.py --difficulty easy
    python evaluation/run_evaluation.py --limit 10
    python evaluation/run_evaluation.py --no-indexes
    python evaluation/run_evaluation.py --output results.json --audit audit_results.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Garante que imports absolutos src.* funcionem
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from evaluation.audit_writer import write_audit
from evaluation.evaluator import EvalRecord, load_ground_truth, run_evaluation
from src.text2sql.few_shot_store import build_few_shot_index
from src.text2sql.pipeline import Text2SQLPipeline
from src.text2sql.schema_linker import build_schema_index

EVAL_DIR = Path(__file__).resolve().parent
GT_PATH = EVAL_DIR / "ground_truth.json"


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------

def build_results_json(records: list[EvalRecord]) -> dict:
    total = len(records)
    passed = sum(1 for r in records if r.passed)
    failed = total - passed

    by_difficulty: dict[str, dict] = defaultdict(lambda: {"total": 0, "passed": 0})
    for r in records:
        by_difficulty[r.difficulty]["total"] += 1
        if r.passed:
            by_difficulty[r.difficulty]["passed"] += 1

    by_diff_out = {}
    for diff, stats in sorted(by_difficulty.items()):
        n = stats["total"]
        p = stats["passed"]
        by_diff_out[diff] = {
            "total": n,
            "passed": p,
            "ex_accuracy": round(p / n * 100, 1) if n else 0.0,
        }

    failures = [
        {
            "id": r.id,
            "question": r.question,
            "difficulty": r.difficulty,
            "agent_sql": r.agent_sql,
            "ground_truth_sql": r.ground_truth_sql,
            "agent_error": r.agent_error,
            "gt_error": r.gt_error,
            "repair_attempts": r.repair_attempts,
        }
        for r in records
        if not r.passed
    ]

    total_latency = sum(r.latency_ms for r in records)
    avg_latency = round(total_latency / total) if total else 0

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total": total,
        "passed": passed,
        "failed": failed,
        "ex_accuracy": round(passed / total * 100, 1) if total else 0.0,
        "by_difficulty": by_diff_out,
        "failures": failures,
        "avg_latency_ms": avg_latency,
        "total_latency_ms": total_latency,
    }


def print_terminal_report(records: list[EvalRecord], results: dict) -> None:
    sep = "=" * 60
    print(f"\n{sep}")
    print("=== EVALUATION — Execution Accuracy (EX) ===")
    print(sep)

    for r in records:
        status = "PASS" if r.passed else ("ERROR" if r.gt_error or r.agent_error else "FAIL")
        label = f"{r.id} [{r.difficulty}]"
        q_preview = r.question[:55] + "..." if len(r.question) > 55 else r.question
        print(f"{label:<18} {status:<6} {r.latency_ms:>5}ms  \"{q_preview}\"")

    print(sep)
    print(f"Total:       {results['total']}")
    print(f"PASS:        {results['passed']} ({results['ex_accuracy']}%)")
    print(f"FAIL:        {results['failed']}")

    errors = sum(1 for r in records if r.gt_error or (not r.passed and r.agent_error))
    print(f"ERROR:       {errors}")

    print(f"\nPor dificuldade:")
    for diff, stats in results["by_difficulty"].items():
        print(f"  {diff:<8}: {stats['passed']}/{stats['total']} ({stats['ex_accuracy']}%)")

    if results["failures"]:
        print(f"\nFalhas:")
        for f in results["failures"]:
            err_msg = ""
            if f["agent_error"]:
                err_msg = f"  pred_error=\"{f['agent_error'][:70]}\""
            elif f["gt_error"]:
                err_msg = f"  gt_error=\"{f['gt_error'][:70]}\""
            agent_sql_preview = (f["agent_sql"] or "")[:80]
            print(f"  {f['id']} [{f['difficulty']}]:{err_msg}")
            if agent_sql_preview:
                print(f"    agent_sql=\"{agent_sql_preview}\"")

    print(sep)
    print(f"Latência média: {results['avg_latency_ms']}ms | Total: {results['total_latency_ms']}ms")
    print(sep)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluation Framework — GT subset matching")
    parser.add_argument("--difficulty", help="Filtrar por dificuldade (easy/medium/hard)")
    parser.add_argument("--limit", type=int, help="Limitar número de exemplos avaliados")
    parser.add_argument("--no-indexes", action="store_true", help="Usar fallback léxico (sem embeddings)")
    parser.add_argument("--output", default="results.json", help="Arquivo de saída dos resultados")
    parser.add_argument("--audit", default="audit_results.json", help="Arquivo de saída do audit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    use_indexes = not args.no_indexes

    print("=" * 60)
    print("EVALUATION — Text-to-SQL Ground Truth (135 exemplos)")
    print(f"Índices semânticos: {'SIM' if use_indexes else 'NÃO (baseline léxico)'}")
    if args.difficulty:
        print(f"Filtro dificuldade: {args.difficulty}")
    if args.limit:
        print(f"Limite: {args.limit} exemplos")
    print("=" * 60)

    # 1. Carrega índices
    schema_store = None
    few_shot_store = None
    if use_indexes:
        print("\nCarregando índices de embeddings...")
        schema_store = build_schema_index()
        few_shot_store = build_few_shot_index()
        print(f"  Schema index: {schema_store.size} docs")
        print(f"  Few-shot index: {few_shot_store.size} docs")

    # 2. Instancia pipeline (1x)
    pipeline = Text2SQLPipeline(
        schema_store=schema_store,
        few_shot_store=few_shot_store,
    )

    # 3. Carrega ground truth
    gt_entries = load_ground_truth(GT_PATH)

    # 4. Aplica filtros
    if args.difficulty:
        gt_entries = [e for e in gt_entries if e.difficulty == args.difficulty]
        print(f"\nFiltrado: {len(gt_entries)} entradas com difficulty='{args.difficulty}'")

    if args.limit:
        gt_entries = gt_entries[:args.limit]
        print(f"Limitado a: {len(gt_entries)} entradas")

    print(f"\nAvaliando {len(gt_entries)} exemplos...")

    # 5. Executa avaliação
    records, audit_entries = run_evaluation(gt_entries, pipeline)

    # 6. Relatório
    results = build_results_json(records)
    print_terminal_report(records, results)

    # 7. Salva results.json
    results_path = EVAL_DIR / args.output
    results_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\nResultados salvos em: {results_path}")

    # 8. Salva audit_results.json
    audit_path = EVAL_DIR / args.audit
    write_audit(audit_path, audit_entries)
    print(f"Audit salvo em:       {audit_path}")


if __name__ == "__main__":
    main()
