#!/usr/bin/env python3
"""
Constrói os índices de embeddings para o pipeline Fase 2.

Execute uma vez (ou quando schema/pares mudarem):
    python scripts/build_indexes.py
    python scripts/build_indexes.py --force   # re-embedda tudo

O que é indexado:
  1. Schema columns index  → .vector_store/schema_columns.json
     - Uma entrada por coluna de cada tabela
     - Uma entrada por tabela (visão geral)
     - Enriquecido com notas do domain_dict

  2. Few-shot pairs index  → .vector_store/few_shot_pairs.json
     - Uma entrada por par NL-SQL (30 pares)
     - Indexado pela pergunta + tags
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.text2sql.schema_linker import build_schema_index
from src.text2sql.few_shot_store import build_few_shot_index


def main():
    force = "--force" in sys.argv

    print("=" * 60)
    print("BUILD INDEXES — Phase 2 Embeddings")
    print(f"Mode: {'FORCE (re-embed all)' if force else 'incremental'}")
    print("=" * 60)

    # ── 1. Schema index ──────────────────────────────────────────────────────
    print("\n[1/2] Construindo índice de schema...")
    t0 = time.monotonic()
    schema_store = build_schema_index(force=force)
    elapsed = time.monotonic() - t0
    print(f"      Docs: {schema_store.size} | Tempo: {elapsed:.1f}s")

    # Smoke test
    results = schema_store.search("taxa de mortalidade hospitalar", top_k=5)
    print("      Top-3 matches para 'taxa de mortalidade hospitalar':")
    for r in results[:3]:
        print(f"        [{r['score']:.3f}] {r['id']}")

    # ── 2. Few-shot index ────────────────────────────────────────────────────
    print("\n[2/2] Construindo índice de pares NL-SQL...")
    t0 = time.monotonic()
    few_shot_store = build_few_shot_index(force=force)
    elapsed = time.monotonic() - t0
    print(f"      Docs: {few_shot_store.size} | Tempo: {elapsed:.1f}s")

    # Smoke test
    results = few_shot_store.search("comparar mortalidade entre estados", top_k=3)
    print("      Top-3 matches para 'comparar mortalidade entre estados':")
    for r in results[:3]:
        print(f"        [{r['score']:.3f}] {r['metadata']['question'][:70]}")

    print("\n" + "=" * 60)
    print("ÍNDICES PRONTOS. Agora execute: python scripts/demo_pipeline.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
