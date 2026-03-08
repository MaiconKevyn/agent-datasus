"""
Few-shot selection semântico via embeddings.

Substitui a seleção léxica da Fase 1 por busca vetorial sobre os pares NL-SQL,
selecionando os exemplos semanticamente mais próximos da query atual.

Por que semântico é melhor que léxico:
  - Captura sinônimos: "óbito" ≃ "morte" ≃ "falecimento"
  - Captura estrutura: "compare X e Y" ≃ "diferença entre X e Y"
  - Não depende de palavras idênticas entre query e exemplo
"""
from __future__ import annotations

from src.text2sql.nl_sql_pairs import NL_SQL_PAIRS, NLSQLPair, format_few_shot
from src.text2sql.vector_store import EmbeddingStore

COLLECTION = "few_shot_pairs"


def build_few_shot_index(force: bool = False) -> EmbeddingStore:
    """
    Constrói (ou recarrega) o índice de pares NL-SQL.
    Indexa a pergunta de cada par (o que será comparado com a query do usuário).
    """
    store = EmbeddingStore(COLLECTION)

    if store.is_built and not force:
        return store

    store.clear()
    for i, pair in enumerate(NL_SQL_PAIRS):
        # Texto indexado: pergunta + tags (enriquece a busca semântica)
        text = f"{pair.question} [tags: {', '.join(pair.tags)}]"
        store.add(
            doc_id=f"pair_{i}",
            text=text,
            metadata={
                "index": i,
                "question": pair.question,
                "difficulty": pair.difficulty,
                "tags": pair.tags,
            },
        )

    n = store.build(force=force)
    print(f"[few_shot_store] Índice '{COLLECTION}': {store.size} docs, {n} embeddings gerados.")
    return store


def get_similar_examples(
    question: str,
    store: EmbeddingStore,
    n: int = 3,
    min_score: float = 0.3,
    exclude_difficulty: list[str] | None = None,
) -> list[NLSQLPair]:
    """
    Retorna os n pares NL-SQL semanticamente mais similares à pergunta.

    Args:
        question: Pergunta do usuário.
        store: Índice já construído.
        n: Número de exemplos a retornar.
        min_score: Score mínimo de similaridade coseno (0-1).
        exclude_difficulty: Exclui pares de determinado nível (ex: ["difícil"]).
    """
    results = store.search(question, top_k=n * 3)  # busca mais para filtrar

    selected: list[NLSQLPair] = []
    for r in results:
        if r["score"] < min_score:
            continue
        idx = r["metadata"]["index"]
        pair = NL_SQL_PAIRS[idx]
        if exclude_difficulty and pair.difficulty in exclude_difficulty:
            continue
        selected.append(pair)
        if len(selected) >= n:
            break

    # Fallback: se não atingiu n exemplos, completa com os primeiros do banco
    if len(selected) < n:
        seen_questions = {p.question for p in selected}
        for pair in NL_SQL_PAIRS:
            if pair.question not in seen_questions:
                if not (exclude_difficulty and pair.difficulty in exclude_difficulty):
                    selected.append(pair)
                    if len(selected) >= n:
                        break

    return selected[:n]
