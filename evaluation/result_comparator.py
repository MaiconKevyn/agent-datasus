"""
result_comparator.py — Normalizador + comparador subset GT ⊆ agent.
"""
from __future__ import annotations


def normalize_value(v) -> str:
    """Normaliza um valor para comparação column-agnostic."""
    if isinstance(v, float):
        return str(round(v, 1))
    if isinstance(v, str):
        return v.strip().lower()
    if v is None:
        return "none"
    return str(v)


def row_to_frozenset(row: tuple) -> frozenset:
    return frozenset(normalize_value(v) for v in row)


def is_subset(gt_rows: list[tuple], agent_rows: list[tuple]) -> bool:
    """
    Verifica se GT ⊆ agent (subset matching column-agnostic).

    Para cada linha do GT (como frozenset de valores normalizados),
    exige que exista pelo menos uma linha agent tal que gt_frozenset ⊆ agent_frozenset.

    Handles: colunas extras no agent, ordem diferente de colunas, linhas extras.
    Edge cases:
      - GT vazio e agent vazio → True
      - GT vazio mas agent não → True (GT trivialmente satisfeito)
      - GT não vazio mas agent vazio → False
    """
    if not gt_rows:
        return True
    if not agent_rows:
        return False

    agent_sets = [row_to_frozenset(r) for r in agent_rows]
    for gt_row in gt_rows:
        gt_set = row_to_frozenset(gt_row)
        if not any(gt_set.issubset(a) for a in agent_sets):
            return False
    return True
