"""
Extração e cache do schema do banco para uso pelo pipeline Text-to-SQL.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .connection import get_connection


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool
    description: str = ""


@dataclass
class TableInfo:
    name: str
    row_count: int
    columns: list[ColumnInfo] = field(default_factory=list)
    description: str = ""
    foreign_keys: list[str] = field(default_factory=list)


# Descrições semânticas manuais — base para schema linking
TABLE_DESCRIPTIONS: dict[str, str] = {
    "internacoes": (
        "Fato principal. Cada linha é uma internação hospitalar no SUS (AIH - Autorização de Internação Hospitalar). "
        "Contém diagnóstico principal (CID-10), datas, valores pagos, desfecho (morte), dados demográficos do paciente "
        "e vínculo com hospital e município de residência."
    ),
    "atendimentos": (
        "Fato secundário. Procedimentos realizados por internação. "
        "Cada N_AIH pode ter múltiplos procedimentos (em média ~2). "
        "Relaciona-se com internacoes via N_AIH e com procedimentos via PROC_REA."
    ),
    "cid": "Dimensão CID-10. Códigos e descrições de diagnósticos internacionais de doenças.",
    "hospital": (
        "Dimensão hospital. Estabelecimentos de saúde identificados por CNES, "
        "com município de movimento (MUNIC_MOV), natureza jurídica e tipo de gestão."
    ),
    "municipios": (
        "Dimensão geográfica. Todos os municípios brasileiros com código IBGE, "
        "coordenadas geográficas e estado (UF). Código de 6 dígitos (codigo_6d) é a chave padrão."
    ),
    "procedimentos": "Dimensão de procedimentos médico-hospitalares (tabela SIGTAP).",
    "especialidade": "Dimensão da especialidade/tipo de leito hospitalar.",
    "raca_cor": "Dimensão raça/cor do paciente (IBGE).",
    "sexo": "Dimensão sexo do paciente.",
    "vincprev": "Dimensão vínculo previdenciário do paciente.",
    "instrucao": "Dimensão nível de instrução/escolaridade do paciente.",
    "etnia": "Dimensão etnia (relevante para pacientes indígenas).",
    "nacionalidade": "Dimensão nacionalidade do paciente.",
    "contraceptivos": "Dimensão método contraceptivo (preenchido em internações obstétricas).",
    "socioeconomico": (
        "Dados socioeconômicos por município e ano (formato pivotado/long). "
        "Métricas: idhm, populacao_total, bolsa_familia_total, mortalidade_infantil_1ano, "
        "esgotamento_sanitario_domicilio, pop_economicamente_ativa, taxa_envelhecimento."
    ),
    "tempo": "Dimensão calendário. Cada data de 2008 a 2023 com ano, mês, trimestre e dia da semana.",
}

# Relacionamentos FK explícitos (para schema linking e geração de JOINs)
FOREIGN_KEYS: dict[str, list[str]] = {
    "internacoes": [
        "internacoes.CNES → hospital.CNES",
        "internacoes.N_AIH → atendimentos.N_AIH (1:N)",
        "internacoes.DIAG_PRINC → cid.CID",
        "internacoes.DIAG_SECUN → cid.CID",
        "internacoes.CID_MORTE → cid.CID",
        "internacoes.ESPEC → especialidade.ESPEC",
        "internacoes.MUNIC_RES → municipios.codigo_6d",
        "internacoes.SEXO → sexo.SEXO",
        "internacoes.RACA_COR → raca_cor.RACA_COR",
        "internacoes.VINCPREV → vincprev.VINCPREV",
        "internacoes.INSTRU → instrucao.INSTRU",
        "internacoes.ETNIA → etnia.ETNIA",
        "internacoes.NACIONAL → nacionalidade.NACIONAL",
        "internacoes.DT_INTER → tempo.data",
    ],
    "atendimentos": [
        "atendimentos.N_AIH → internacoes.N_AIH",
        "atendimentos.PROC_REA → procedimentos.PROC_REA",
    ],
    "hospital": [
        "hospital.MUNIC_MOV → municipios.codigo_6d",
    ],
    "municipios": [
        "municipios.codigo_6d → socioeconomico.codigo_6d (1:N por ano/metrica)",
    ],
}


def get_schema_info() -> list[TableInfo]:
    """
    Extrai schema completo do banco e anota com descrições semânticas.
    Resultado pode ser serializado para o prompt do LLM.
    """
    tables: list[TableInfo] = []

    with get_connection() as conn:
        table_names = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        ]

        for name in table_names:
            count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            cols_raw = conn.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                f"WHERE table_name = '{name}' ORDER BY ordinal_position"
            ).fetchall()

            columns = [
                ColumnInfo(
                    name=c[0],
                    data_type=c[1],
                    nullable=(c[2] == "YES"),
                )
                for c in cols_raw
            ]

            tables.append(
                TableInfo(
                    name=name,
                    row_count=count,
                    columns=columns,
                    description=TABLE_DESCRIPTIONS.get(name, ""),
                    foreign_keys=FOREIGN_KEYS.get(name, []),
                )
            )

    return tables


def build_schema_prompt(tables: Optional[list[TableInfo]] = None) -> str:
    """
    Serializa o schema em formato DDL-like legível por LLMs.
    Inclui descrições semânticas e FKs para melhor schema linking.
    """
    if tables is None:
        tables = get_schema_info()

    lines: list[str] = ["-- Schema: SIH-RD DATASUS (SUS Hospital Information System)", "-- States: RS + MA | Period: 2008-2023 | Engine: DuckDB\n"]

    for t in tables:
        if t.description:
            lines.append(f"-- {t.description}")
        lines.append(f"-- Rows: {t.row_count:,}")
        lines.append(f"CREATE TABLE {t.name} (")
        col_defs = []
        for c in t.columns:
            null_str = "" if c.nullable else " NOT NULL"
            col_defs.append(f"    {c.name} {c.data_type}{null_str}")
        lines.append(",\n".join(col_defs))
        lines.append(");")
        if t.foreign_keys:
            for fk in t.foreign_keys:
                lines.append(f"-- FK: {fk}")
        lines.append("")

    return "\n".join(lines)
