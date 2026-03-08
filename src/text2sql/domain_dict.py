"""
Dicionário de domínio: mapeamentos NL → schema.

Serve como base para schema linking determinístico e enriquecimento do prompt.
Cada entrada mapeia expressões em linguagem natural para a coluna, tabela e
valor correspondentes no banco, com notas de uso para o LLM.
"""

from dataclasses import dataclass, field


@dataclass
class DomainEntry:
    """Mapeamento de um conceito de domínio para o schema."""
    terms: list[str]           # expressões NL que ativam este mapeamento
    table: str
    column: str
    value: str | None = None   # valor fixo, quando a entrada é um filtro
    note: str = ""             # instrução ao LLM sobre como usar


# ---------------------------------------------------------------------------
# DICIONÁRIO PRINCIPAL
# ---------------------------------------------------------------------------

DOMAIN_DICT: list[DomainEntry] = [

    # ── DESFECHO ────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["morte", "óbito", "morreu", "mortal", "falecimento", "faleceu",
               "mortality", "death", "died", "fatal"],
        table="internacoes", column="MORTE", value="TRUE",
        note="MORTE é BOOLEAN. Use MORTE=TRUE para filtrar óbitos. "
             "Para taxa: SUM(MORTE::INT)/COUNT(*). Nunca use MORTE=1.",
    ),
    DomainEntry(
        terms=["taxa de mortalidade", "taxa de óbito", "mortalidade hospitalar"],
        table="internacoes", column="MORTE",
        note="Calcule como: ROUND(100.0 * SUM(MORTE::INT) / COUNT(*), 2). "
             "Sempre apresente como percentual.",
    ),
    DomainEntry(
        terms=["diagnóstico de morte", "causa da morte", "causa mortis", "cid de morte"],
        table="internacoes", column="CID_MORTE",
        note="CID_MORTE registra o diagnóstico no momento do óbito. "
             "Diferente de DIAG_PRINC (diagnóstico de entrada).",
    ),

    # ── DIAGNÓSTICO ──────────────────────────────────────────────────────────
    DomainEntry(
        terms=["diagnóstico", "diagnóstico principal", "cid principal", "doença principal",
               "causa da internação"],
        table="internacoes", column="DIAG_PRINC",
        note="DIAG_PRINC contém o código CID-10 do diagnóstico principal. "
             "Faça JOIN com tabela cid ON internacoes.DIAG_PRINC = cid.CID "
             "para obter a descrição.",
    ),
    DomainEntry(
        terms=["diagnóstico secundário", "comorbidade", "cid secundário"],
        table="internacoes", column="DIAG_SECUN",
        note="DIAG_SECUN é o diagnóstico secundário. Também faz JOIN com cid.CID.",
    ),
    DomainEntry(
        terms=["descrição do diagnóstico", "nome da doença", "nome do cid"],
        table="cid", column="CD_DESCRICAO",
        note="Sempre faça JOIN: internacoes.DIAG_PRINC = cid.CID.",
    ),

    # ── CUSTO / VALOR ────────────────────────────────────────────────────────
    DomainEntry(
        terms=["custo", "valor", "gasto", "despesa", "pagamento", "custo total",
               "valor total", "quanto custou", "cost", "expense"],
        table="internacoes", column="VAL_TOT",
        note="VAL_TOT é o valor total pago pela internação. "
             "NÃO use VAL_SH (só serviços hospitalares) nem VAL_SP (só profissionais). "
             "Para total: SUM(VAL_TOT). Para média: AVG(VAL_TOT).",
    ),
    DomainEntry(
        terms=["custo de uti", "valor de uti", "custo uti"],
        table="internacoes", column="VAL_UTI",
        note="VAL_UTI é o valor pago especificamente pela UTI.",
    ),

    # ── TEMPO ────────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["data de internação", "data de entrada", "admissão"],
        table="internacoes", column="DT_INTER",
        note="DT_INTER é a data de entrada. Tipo DATE. "
             "Use EXTRACT(year FROM DT_INTER) para ano.",
    ),
    DomainEntry(
        terms=["data de saída", "data de alta", "alta hospitalar"],
        table="internacoes", column="DT_SAIDA",
        note="DT_SAIDA é a data de saída/alta. Tipo DATE.",
    ),
    DomainEntry(
        terms=["dias de internação", "tempo de internação", "permanência",
               "dias internado", "length of stay"],
        table="internacoes", column="DIAS_PERM",
        note="DIAS_PERM é a duração em dias. Para média: AVG(DIAS_PERM).",
    ),
    DomainEntry(
        terms=["pandemia", "covid", "2020", "2021"],
        table="internacoes", column="DT_INTER",
        note="Para análises de pandemia COVID-19, filtre: "
             "EXTRACT(year FROM DT_INTER) IN (2020, 2021).",
    ),

    # ── SEXO ─────────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["feminino", "mulher", "mulheres", "female", "women"],
        table="internacoes", column="SEXO", value="3",
        note="SEXO=3 para FEMININO. ATENÇÃO: não é 2, é 3.",
    ),
    DomainEntry(
        terms=["masculino", "homem", "homens", "male", "men"],
        table="internacoes", column="SEXO", value="1",
        note="SEXO=1 para MASCULINO.",
    ),

    # ── IDADE ────────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["idade", "faixa etária", "idoso", "criança", "adulto",
               "neonato", "pediátrico"],
        table="internacoes", column="IDADE",
        note="IDADE é em anos. "
             "Idoso: IDADE >= 60. Criança/pediátrico: IDADE < 18. "
             "Neonato: IDADE = 0.",
    ),

    # ── LOCALIZAÇÃO ──────────────────────────────────────────────────────────
    DomainEntry(
        terms=["município do paciente", "cidade do paciente", "residência do paciente",
               "onde mora", "município de residência"],
        table="internacoes", column="MUNIC_RES",
        note="MUNIC_RES é o município de residência do paciente. "
             "Faça JOIN: internacoes.MUNIC_RES = municipios.codigo_6d.",
    ),
    DomainEntry(
        terms=["município do hospital", "cidade do hospital", "local do atendimento",
               "onde foi atendido", "município de movimento"],
        table="hospital", column="MUNIC_MOV",
        note="MUNIC_MOV é o município onde fica o hospital. "
             "Faça JOIN: hospital.MUNIC_MOV = municipios.codigo_6d.",
    ),
    DomainEntry(
        terms=["estado", "uf", "estado do paciente", "rs", "ma",
               "rio grande do sul", "maranhão"],
        table="municipios", column="estado",
        note="Banco contém APENAS RS e MA. "
             "Filtre via JOIN com municipios: municipios.estado = 'RS'.",
    ),
    DomainEntry(
        terms=["nome do município", "nome da cidade"],
        table="municipios", column="nome",
        note="JOIN: internacoes.MUNIC_RES = municipios.codigo_6d. "
             "Usar municipios.nome para exibição.",
    ),

    # ── HOSPITAL ─────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["hospital", "estabelecimento", "cnes", "unidade de saúde"],
        table="hospital", column="CNES",
        note="CNES é o identificador único do hospital. "
             "JOIN: internacoes.CNES = hospital.CNES.",
    ),
    DomainEntry(
        terms=["natureza do hospital", "tipo de hospital", "hospital público",
               "hospital privado", "filantrópico"],
        table="hospital", column="NATUREZA",
        note="NATUREZA: '0'=público, '20'=privado, '50'=filantrópico sem fins lucrativos, "
             "'61'=filantrópico com fins lucrativos.",
    ),
    DomainEntry(
        terms=["gestão do hospital", "gestão estadual", "gestão municipal"],
        table="hospital", column="GESTAO",
        note="GESTAO: '1'=gestão estadual, '2'=gestão municipal.",
    ),

    # ── ESPECIALIDADE ────────────────────────────────────────────────────────
    DomainEntry(
        terms=["especialidade", "tipo de leito", "cirurgia", "clínica",
               "obstetrícia", "pediatria", "psiquiatria", "uti"],
        table="especialidade", column="DESCRICAO",
        note="JOIN: internacoes.ESPEC = especialidade.ESPEC. "
             "Cirúrgico=1, Obstétrico=2, Clínico=3, Crônico=4, "
             "Psiquiatria=5, Pediátrico=7, UTI Adulto=74-76.",
    ),
    DomainEntry(
        terms=["internação em uti", "uti", "terapia intensiva", "dias de uti"],
        table="internacoes", column="UTI_INT_TO",
        note="UTI_INT_TO = número de dias em UTI. "
             "0 significa sem UTI. Para filtrar com UTI: UTI_INT_TO > 0.",
    ),

    # ── PROCEDIMENTOS ────────────────────────────────────────────────────────
    DomainEntry(
        terms=["procedimento", "procedimento realizado", "cirurgia realizada",
               "parto", "operação"],
        table="atendimentos", column="PROC_REA",
        note="Para listar procedimentos, use tabela atendimentos com JOIN procedimentos. "
             "ATENÇÃO: para CONTAR internações, use tabela internacoes, não atendimentos.",
    ),
    DomainEntry(
        terms=["nome do procedimento", "descrição do procedimento"],
        table="procedimentos", column="NOME_PROC",
        note="JOIN: atendimentos.PROC_REA = procedimentos.PROC_REA.",
    ),

    # ── RAÇA/COR ─────────────────────────────────────────────────────────────
    DomainEntry(
        terms=["raça", "cor", "raça/cor", "etnia racial",
               "branco", "preto", "pardo", "amarelo", "indígena"],
        table="raca_cor", column="DESCRICAO",
        note="JOIN: internacoes.RACA_COR = raca_cor.RACA_COR. "
             "Valores: 1=BRANCA, 2=PRETA, 3=PARDA, 4=AMARELA, 5=INDIGENA, 99=SEM INFO.",
    ),

    # ── VÍNCULO PREVIDENCIÁRIO ────────────────────────────────────────────────
    DomainEntry(
        terms=["vínculo previdenciário", "situação trabalhista", "empregado",
               "aposentado", "desempregado", "autônomo"],
        table="vincprev", column="DESCRICAO",
        note="JOIN: internacoes.VINCPREV = vincprev.VINCPREV. "
             "Valores: 1=AUTONOMO, 2=DESEMPREGADO, 3=APOSENTADO, "
             "4=NAO SEGURADO, 5=EMPREGADO, 6=EMPREGADOR.",
    ),

    # ── ESCOLARIDADE ─────────────────────────────────────────────────────────
    DomainEntry(
        terms=["escolaridade", "instrução", "nível de educação",
               "analfabeto", "ensino fundamental", "ensino médio", "superior"],
        table="instrucao", column="DESCRICAO",
        note="JOIN: internacoes.INSTRU = instrucao.INSTRU.",
    ),

    # ── SOCIOECONÔMICO ───────────────────────────────────────────────────────
    DomainEntry(
        terms=["idhm", "idh", "índice de desenvolvimento humano"],
        table="socioeconomico", column="valor",
        value="idhm",
        note="Filtre: metrica = 'idhm'. JOIN: municipios.codigo_6d = socioeconomico.codigo_6d. "
             "Disponível para anos censitários (ex: 2010). "
             "Para classificar: IDH < 0.55=muito baixo, 0.55-0.699=baixo, "
             "0.7-0.799=médio, >= 0.8=alto.",
    ),
    DomainEntry(
        terms=["população", "tamanho da população", "total de habitantes"],
        table="socioeconomico", column="valor",
        value="populacao_total",
        note="Filtre: metrica = 'populacao_total'.",
    ),
    DomainEntry(
        terms=["bolsa família", "assistência social", "beneficiários bolsa"],
        table="socioeconomico", column="valor",
        value="bolsa_familia_total",
        note="Filtre: metrica = 'bolsa_familia_total'.",
    ),
    DomainEntry(
        terms=["mortalidade infantil", "óbito infantil"],
        table="socioeconomico", column="valor",
        value="mortalidade_infantil_1ano",
        note="Filtre: metrica = 'mortalidade_infantil_1ano'.",
    ),
    DomainEntry(
        terms=["saneamento", "esgotamento sanitário", "saneamento básico"],
        table="socioeconomico", column="valor",
        value="esgotamento_sanitario_domicilio",
        note="Filtre: metrica = 'esgotamento_sanitario_domicilio'.",
    ),
    DomainEntry(
        terms=["taxa de envelhecimento", "população idosa", "envelhecimento populacional"],
        table="socioeconomico", column="valor",
        value="taxa_envelhecimento",
        note="Filtre: metrica = 'taxa_envelhecimento'.",
    ),

    # ── CONTAGEM / VOLUME ────────────────────────────────────────────────────
    DomainEntry(
        terms=["número de internações", "quantidade de internações", "total de internações",
               "quantas internações", "volume de internações"],
        table="internacoes", column="N_AIH",
        note="Use COUNT(*) FROM internacoes. "
             "NUNCA conte internações via tabela atendimentos.",
    ),
    DomainEntry(
        terms=["número de procedimentos", "quantidade de procedimentos",
               "procedimentos realizados"],
        table="atendimentos", column="id_atendimento",
        note="Use COUNT(*) FROM atendimentos.",
    ),

    # ── OBSTÉTRICO ───────────────────────────────────────────────────────────
    DomainEntry(
        terms=["parto normal", "parto vaginal", "parto espontâneo"],
        table="internacoes", column="DIAG_PRINC",
        note="Partos normais: DIAG_PRINC LIKE 'O80%'. "
             "Parto cesáreo: DIAG_PRINC LIKE 'O82%'.",
    ),
    DomainEntry(
        terms=["cesariana", "parto cesáreo", "cesárea"],
        table="internacoes", column="DIAG_PRINC",
        note="Cesárea: DIAG_PRINC LIKE 'O82%'.",
    ),
    DomainEntry(
        terms=["contraceptivo", "método contraceptivo", "anticoncepcional"],
        table="contraceptivos", column="DESCRICAO",
        note="JOIN: internacoes.CONTRACEP1 = contraceptivos.CONTRACEPTIVO "
             "ou internacoes.CONTRACEP2 = contraceptivos.CONTRACEPTIVO.",
    ),
]


def get_relevant_entries(query: str) -> list[DomainEntry]:
    """
    Retorna entradas do dicionário relevantes para a query (matching simples por termos).
    Para produção, substituir por busca por embedding.
    """
    q = query.lower()
    matched = []
    seen_columns = set()

    for entry in DOMAIN_DICT:
        if any(term in q for term in entry.terms):
            key = (entry.table, entry.column)
            if key not in seen_columns:
                seen_columns.add(key)
                matched.append(entry)

    return matched


def format_domain_rules(entries: list[DomainEntry]) -> str:
    """Serializa entradas relevantes como bloco de regras para o prompt."""
    if not entries:
        return ""
    lines = ["REGRAS DO DOMÍNIO (críticas para esta query):"]
    for e in entries:
        col_ref = f"{e.table}.{e.column}"
        if e.value:
            col_ref += f" = {e.value}"
        lines.append(f"- {col_ref}: {e.note}")
    return "\n".join(lines)
