"""
Schema linking semântico via embeddings.

Substitui o matching léxico da Fase 1 por busca vetorial sobre o schema,
selecionando apenas as tabelas e colunas relevantes para cada query.

Vantagem sobre schema completo:
  - Reduz tokens no prompt (~50-70% menos)
  - Reduz alucinações com tabelas não relacionadas à query
  - Permite escalar para schemas com dezenas de tabelas

Documentos indexados (por coluna):
  "<table>.<column>: <description_semântica> | tipo: <dtype> | <notas_domínio>"

Documentos indexados (por tabela):
  "<table> [tabela]: <TABLE_DESCRIPTION>"
"""
from __future__ import annotations

from src.db.schema import (
    TableInfo,
    get_schema_info,
    build_schema_prompt,
    TABLE_DESCRIPTIONS,
    FOREIGN_KEYS,
)
from src.text2sql.domain_dict import DOMAIN_DICT
from src.text2sql.vector_store import EmbeddingStore

COLLECTION = "schema_columns"

# Notas extras por coluna crítica (complementam o domain_dict no texto indexado)
_COLUMN_NOTES: dict[str, str] = {
    "internacoes.SEXO": "SEXO=1 MASCULINO, SEXO=3 FEMININO (não existe 2)",
    "internacoes.MORTE": "BOOLEAN. Filtrar: MORTE=TRUE. Agregar: SUM(MORTE::INT)",
    "internacoes.VAL_TOT": "Valor total pago. Use este, não VAL_SH nem VAL_SP",
    "internacoes.MUNIC_RES": "Município de residência do paciente. FK → municipios.codigo_6d",
    "hospital.MUNIC_MOV": "Município onde fica o hospital. FK → municipios.codigo_6d",
    "municipios.codigo_6d": "Chave FK padrão (6 dígitos). NÃO usar codigo_ibge para joins",
    "internacoes.DIAG_PRINC": "Diagnóstico principal CID-10. JOIN com cid.CID",
    "internacoes.DIAG_SECUN": "Diagnóstico secundário CID-10. JOIN com cid.CID",
    "internacoes.DIAS_PERM": "Duração da internação em dias",
    "internacoes.UTI_INT_TO": "Dias em UTI. 0 = sem UTI (não NULL)",
    "socioeconomico.metrica": (
        "Tabela long format. Sempre filtre: metrica IN "
        "('idhm','populacao_total','bolsa_familia_total',"
        "'mortalidade_infantil_1ano','esgotamento_sanitario_domicilio',"
        "'pop_economicamente_ativa','taxa_envelhecimento')"
    ),
}

# Domain dict notes por (table, column) para enriquecer o texto indexado
_DOMAIN_NOTES_BY_COL: dict[tuple[str, str], list[str]] = {}
for _entry in DOMAIN_DICT:
    _key = (_entry.table, _entry.column)
    if _key not in _DOMAIN_NOTES_BY_COL:
        _DOMAIN_NOTES_BY_COL[_key] = []
    _DOMAIN_NOTES_BY_COL[_key].append(f"[sinônimos: {', '.join(_entry.terms[:5])}]")


def _build_column_text(table: TableInfo, col_name: str, col_dtype: str) -> str:
    """Monta o texto de um documento de coluna para indexação."""
    col_key = f"{table.name}.{col_name}"
    parts = [col_key]

    # Nota específica da coluna
    if col_key in _COLUMN_NOTES:
        parts.append(_COLUMN_NOTES[col_key])

    # Notas do domain dict
    domain_notes = _DOMAIN_NOTES_BY_COL.get((table.name, col_name), [])
    parts.extend(domain_notes)

    # Tipo de dado
    parts.append(f"tipo: {col_dtype}")

    # Descrição da tabela (contexto)
    if table.description:
        parts.append(f"tabela: {table.description[:120]}")

    return " | ".join(parts)


def build_schema_index(force: bool = False) -> EmbeddingStore:
    """
    Constrói (ou recarrega) o índice de colunas do schema.
    force=True re-embedda tudo mesmo que o índice já exista.
    """
    store = EmbeddingStore(COLLECTION)

    if store.is_built and not force:
        return store

    store.clear()
    tables = get_schema_info()

    for table in tables:
        # Documento por tabela
        table_text = f"{table.name} [tabela]: {table.description or table.name}"
        store.add(
            doc_id=f"table::{table.name}",
            text=table_text,
            metadata={"type": "table", "table": table.name},
        )

        # Documento por coluna
        for col in table.columns:
            text = _build_column_text(table, col.name, col.data_type)
            store.add(
                doc_id=f"col::{table.name}.{col.name}",
                text=text,
                metadata={
                    "type": "column",
                    "table": table.name,
                    "column": col.name,
                    "dtype": col.data_type,
                },
            )

    n = store.build(force=force)
    print(f"[schema_linker] Índice '{COLLECTION}': {store.size} docs, {n} embeddings gerados.")
    return store


# ── Linking ──────────────────────────────────────────────────────────────────

def link_schema(
    question: str,
    store: EmbeddingStore,
    top_k_cols: int = 20,
    always_include: list[str] | None = None,
) -> tuple[str, list[str]]:
    """
    Retorna (schema_prompt_seletivo, tabelas_relevantes).

    Estratégia:
      1. Busca os top_k_cols documentos mais similares (colunas + tabelas)
      2. Extrai o conjunto de tabelas únicas encontradas
      3. Sempre inclui internacoes (tabela fato central)
      4. Gera prompt com apenas as tabelas relevantes + seus FKs
    """
    results = store.search(question, top_k=top_k_cols)

    # Tabelas encontradas via embeddings
    relevant_tables: set[str] = set()
    for r in results:
        if r["score"] > 0.25:  # threshold mínimo de similaridade
            relevant_tables.add(r["metadata"]["table"])

    # Sempre incluir internacoes (fato central) e tabelas forçadas
    relevant_tables.add("internacoes")
    for t in (always_include or []):
        relevant_tables.add(t)

    # Expande com tabelas necessárias para FKs das tabelas encontradas
    relevant_tables = _expand_with_fk_deps(relevant_tables)

    # Gera schema seletivo
    all_tables = get_schema_info()
    selected = [t for t in all_tables if t.name in relevant_tables]
    schema_prompt = build_schema_prompt(selected)

    return schema_prompt, sorted(relevant_tables)


def _expand_with_fk_deps(tables: set[str]) -> set[str]:
    """
    Adiciona tabelas necessárias para resolver FKs das tabelas selecionadas.
    Ex: se 'internacoes' está no set e usa 'municipios' via MUNIC_RES,
    adiciona 'municipios' ao set.
    """
    expanded = set(tables)

    # Mapeamento simplificado de dependências FK
    fk_deps: dict[str, list[str]] = {
        "internacoes": ["hospital", "cid", "municipios", "especialidade"],
        "atendimentos": ["internacoes", "procedimentos"],
        "hospital": ["municipios"],
        "municipios": ["socioeconomico"],
    }

    for table in list(tables):
        for dep in fk_deps.get(table, []):
            if dep in tables:
                expanded.add(dep)

    return expanded
