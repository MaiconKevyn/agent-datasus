"""
Pares NL-SQL anotados manualmente para few-shot learning.

Cobre os principais padrões de query do domínio SIH-RD:
  - Agregações simples (COUNT, SUM, AVG)
  - Filtros temporais (ano, período, pandemia)
  - Filtros demográficos (sexo, raça, faixa etária)
  - Filtros clínicos (CID, especialidade, UTI)
  - Ranking e top-N
  - Séries temporais
  - Joins socioeconômicos
  - Comparações entre estados
  - Queries obstétricas
"""

from dataclasses import dataclass


@dataclass
class NLSQLPair:
    question: str
    sql: str
    difficulty: str   # "simples" | "médio" | "difícil"
    tags: list[str]


NL_SQL_PAIRS: list[NLSQLPair] = [

    # ── SIMPLES: CONTAGENS BÁSICAS ──────────────────────────────────────────

    NLSQLPair(
        question="Quantas internações existem no banco?",
        sql="SELECT COUNT(*) AS total_internacoes FROM internacoes;",
        difficulty="simples",
        tags=["contagem", "internacoes"],
    ),
    NLSQLPair(
        question="Qual o total de internações por ano?",
        sql="""
SELECT EXTRACT(year FROM DT_INTER) AS ano,
       COUNT(*) AS total_internacoes
FROM internacoes
WHERE DT_INTER IS NOT NULL
GROUP BY ano
ORDER BY ano;
""".strip(),
        difficulty="simples",
        tags=["contagem", "temporal", "série temporal"],
    ),
    NLSQLPair(
        question="Quantas internações foram registradas em 2022?",
        sql="""
SELECT COUNT(*) AS total_internacoes
FROM internacoes
WHERE EXTRACT(year FROM DT_INTER) = 2022;
""".strip(),
        difficulty="simples",
        tags=["contagem", "filtro temporal"],
    ),
    NLSQLPair(
        question="Quantas internações ocorreram no Rio Grande do Sul?",
        sql="""
SELECT COUNT(*) AS total_internacoes
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
WHERE m.estado = 'RS';
""".strip(),
        difficulty="simples",
        tags=["contagem", "filtro geográfico", "join"],
    ),
    NLSQLPair(
        question="Quantos óbitos hospitalares foram registrados no total?",
        sql="""
SELECT SUM(MORTE::INT) AS total_obitos
FROM internacoes;
""".strip(),
        difficulty="simples",
        tags=["morte", "agregação"],
    ),

    # ── SIMPLES: VALORES E CUSTOS ───────────────────────────────────────────

    NLSQLPair(
        question="Qual o custo total pago pelo SUS em internações?",
        sql="""
SELECT ROUND(SUM(VAL_TOT), 2) AS custo_total_reais
FROM internacoes;
""".strip(),
        difficulty="simples",
        tags=["custo", "agregação"],
    ),
    NLSQLPair(
        question="Qual o custo médio por internação por estado?",
        sql="""
SELECT m.estado,
       COUNT(*) AS internacoes,
       ROUND(AVG(i.VAL_TOT), 2) AS custo_medio
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
GROUP BY m.estado
ORDER BY custo_medio DESC;
""".strip(),
        difficulty="simples",
        tags=["custo", "join", "agrupamento geográfico"],
    ),
    NLSQLPair(
        question="Qual o tempo médio de internação em dias?",
        sql="""
SELECT ROUND(AVG(DIAS_PERM), 2) AS media_dias_internacao
FROM internacoes;
""".strip(),
        difficulty="simples",
        tags=["permanência", "média"],
    ),

    # ── MÉDIO: FILTROS DEMOGRÁFICOS ─────────────────────────────────────────

    NLSQLPair(
        question="Quantas internações de mulheres ocorreram em 2023?",
        sql="""
SELECT COUNT(*) AS internacoes_femininas
FROM internacoes
WHERE SEXO = 3
  AND EXTRACT(year FROM DT_INTER) = 2023;
""".strip(),
        difficulty="médio",
        tags=["sexo", "filtro temporal", "demográfico"],
    ),
    NLSQLPair(
        question="Qual a distribuição de internações por raça/cor?",
        sql="""
SELECT r.DESCRICAO AS raca_cor,
       COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM internacoes i
JOIN raca_cor r ON i.RACA_COR = r.RACA_COR
GROUP BY r.DESCRICAO
ORDER BY total DESC;
""".strip(),
        difficulty="médio",
        tags=["raça", "distribuição", "join", "window function"],
    ),
    NLSQLPair(
        question="Quantas internações de idosos (60 anos ou mais) ocorreram por ano?",
        sql="""
SELECT EXTRACT(year FROM DT_INTER) AS ano,
       COUNT(*) AS internacoes_idosos
FROM internacoes
WHERE IDADE >= 60
  AND DT_INTER IS NOT NULL
GROUP BY ano
ORDER BY ano;
""".strip(),
        difficulty="médio",
        tags=["faixa etária", "idoso", "série temporal"],
    ),
    NLSQLPair(
        question="Qual o perfil de internações por vínculo previdenciário?",
        sql="""
SELECT v.DESCRICAO AS vinculo_previdenciario,
       COUNT(*) AS total,
       ROUND(AVG(i.DIAS_PERM), 2) AS media_dias,
       ROUND(AVG(i.VAL_TOT), 2) AS custo_medio
FROM internacoes i
JOIN vincprev v ON i.VINCPREV = v.VINCPREV
GROUP BY v.DESCRICAO
ORDER BY total DESC;
""".strip(),
        difficulty="médio",
        tags=["vínculo previdenciário", "demográfico", "join"],
    ),

    # ── MÉDIO: FILTROS CLÍNICOS ─────────────────────────────────────────────

    NLSQLPair(
        question="Quais os 10 diagnósticos principais mais frequentes?",
        sql="""
SELECT i.DIAG_PRINC AS cid,
       c.CD_DESCRICAO AS descricao,
       COUNT(*) AS total_internacoes
FROM internacoes i
LEFT JOIN cid c ON i.DIAG_PRINC = c.CID
GROUP BY i.DIAG_PRINC, c.CD_DESCRICAO
ORDER BY total_internacoes DESC
LIMIT 10;
""".strip(),
        difficulty="médio",
        tags=["diagnóstico", "ranking", "join", "top-N"],
    ),
    NLSQLPair(
        question="Qual a taxa de mortalidade por especialidade?",
        sql="""
SELECT e.DESCRICAO AS especialidade,
       COUNT(*) AS internacoes,
       SUM(i.MORTE::INT) AS obitos,
       ROUND(100.0 * SUM(i.MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct
FROM internacoes i
JOIN especialidade e ON i.ESPEC = e.ESPEC
GROUP BY e.DESCRICAO
ORDER BY taxa_mortalidade_pct DESC;
""".strip(),
        difficulty="médio",
        tags=["morte", "especialidade", "taxa", "join"],
    ),
    NLSQLPair(
        question="Quantas internações por pneumonia aconteceram por ano?",
        sql="""
SELECT EXTRACT(year FROM DT_INTER) AS ano,
       COUNT(*) AS internacoes_pneumonia
FROM internacoes
WHERE DIAG_PRINC IN ('J180', 'J189', 'J181', 'J182', 'J188')
  AND DT_INTER IS NOT NULL
GROUP BY ano
ORDER BY ano;
""".strip(),
        difficulty="médio",
        tags=["diagnóstico", "CID", "série temporal", "filtro clínico"],
    ),
    NLSQLPair(
        question="Qual o custo médio de internações cirúrgicas vs clínicas?",
        sql="""
SELECT e.DESCRICAO AS especialidade,
       COUNT(*) AS internacoes,
       ROUND(AVG(i.VAL_TOT), 2) AS custo_medio
FROM internacoes i
JOIN especialidade e ON i.ESPEC = e.ESPEC
WHERE e.ESPEC IN (1, 3)
GROUP BY e.DESCRICAO;
""".strip(),
        difficulty="médio",
        tags=["especialidade", "custo", "comparação"],
    ),
    NLSQLPair(
        question="Quantas internações tiveram passagem por UTI?",
        sql="""
SELECT COUNT(*) AS internacoes_com_uti
FROM internacoes
WHERE UTI_INT_TO > 0;
""".strip(),
        difficulty="médio",
        tags=["UTI", "filtro"],
    ),
    NLSQLPair(
        question="Qual o tempo médio de internação em UTI por especialidade?",
        sql="""
SELECT e.DESCRICAO AS especialidade,
       COUNT(*) AS internacoes_com_uti,
       ROUND(AVG(i.UTI_INT_TO), 2) AS media_dias_uti,
       ROUND(AVG(i.VAL_UTI), 2) AS custo_medio_uti
FROM internacoes i
JOIN especialidade e ON i.ESPEC = e.ESPEC
WHERE i.UTI_INT_TO > 0
GROUP BY e.DESCRICAO
ORDER BY media_dias_uti DESC;
""".strip(),
        difficulty="médio",
        tags=["UTI", "especialidade", "permanência"],
    ),

    # ── MÉDIO: RANKING E TOP-N ──────────────────────────────────────────────

    NLSQLPair(
        question="Quais os 5 hospitais com maior número de internações no MA?",
        sql="""
SELECT h.CNES,
       m.nome AS municipio,
       COUNT(*) AS total_internacoes
FROM internacoes i
JOIN hospital h ON i.CNES = h.CNES
JOIN municipios m ON h.MUNIC_MOV = m.codigo_6d
WHERE m.estado = 'MA'
GROUP BY h.CNES, m.nome
ORDER BY total_internacoes DESC
LIMIT 5;
""".strip(),
        difficulty="médio",
        tags=["hospital", "ranking", "top-N", "filtro geográfico"],
    ),
    NLSQLPair(
        question="Quais os municípios com maior taxa de mortalidade hospitalar no RS?",
        sql="""
SELECT m.nome AS municipio,
       COUNT(*) AS internacoes,
       SUM(i.MORTE::INT) AS obitos,
       ROUND(100.0 * SUM(i.MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
WHERE m.estado = 'RS'
GROUP BY m.nome
HAVING COUNT(*) >= 100
ORDER BY taxa_mortalidade_pct DESC
LIMIT 10;
""".strip(),
        difficulty="médio",
        tags=["morte", "ranking", "filtro geográfico", "HAVING"],
    ),

    # ── MÉDIO: OBSTÉTRICO ───────────────────────────────────────────────────

    NLSQLPair(
        question="Qual a proporção de partos normais vs cesarianas por ano?",
        sql="""
SELECT EXTRACT(year FROM DT_INTER) AS ano,
       SUM(CASE WHEN DIAG_PRINC LIKE 'O80%' THEN 1 ELSE 0 END) AS parto_normal,
       SUM(CASE WHEN DIAG_PRINC LIKE 'O82%' THEN 1 ELSE 0 END) AS cesariana,
       ROUND(100.0 * SUM(CASE WHEN DIAG_PRINC LIKE 'O82%' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN DIAG_PRINC LIKE 'O80%' OR DIAG_PRINC LIKE 'O82%' THEN 1 ELSE 0 END), 0), 2)
             AS pct_cesariana
FROM internacoes
WHERE DT_INTER IS NOT NULL
GROUP BY ano
ORDER BY ano;
""".strip(),
        difficulty="médio",
        tags=["obstetrícia", "parto", "CID", "série temporal"],
    ),

    # ── DIFÍCIL: CRUZAMENTOS SOCIOECONÔMICOS ────────────────────────────────

    NLSQLPair(
        question="Qual a relação entre IDH do município e taxa de mortalidade hospitalar?",
        sql="""
SELECT m.nome AS municipio,
       m.estado,
       s.valor AS idhm_2010,
       COUNT(i.N_AIH) AS internacoes,
       ROUND(100.0 * SUM(i.MORTE::INT) / NULLIF(COUNT(*), 0), 2) AS taxa_mortalidade_pct
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
JOIN socioeconomico s ON m.codigo_6d = s.codigo_6d
WHERE s.metrica = 'idhm'
  AND s.ano = 2010
GROUP BY m.nome, m.estado, s.valor
HAVING COUNT(*) >= 500
ORDER BY idhm_2010;
""".strip(),
        difficulty="difícil",
        tags=["socioeconômico", "IDH", "morte", "correlação", "join múltiplo"],
    ),
    NLSQLPair(
        question="Compare a mortalidade hospitalar entre municípios de alto e baixo IDH no RS",
        sql="""
WITH municipios_idh AS (
    SELECT s.codigo_6d,
           s.valor AS idhm,
           CASE
               WHEN s.valor >= 0.7 THEN 'alto'
               ELSE 'baixo'
           END AS faixa_idh
    FROM socioeconomico s
    WHERE s.metrica = 'idhm' AND s.ano = 2010
)
SELECT mi.faixa_idh,
       COUNT(*) AS internacoes,
       SUM(i.MORTE::INT) AS obitos,
       ROUND(100.0 * SUM(i.MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct,
       ROUND(AVG(i.DIAS_PERM), 2) AS media_dias_perm
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
JOIN municipios_idh mi ON m.codigo_6d = mi.codigo_6d
WHERE m.estado = 'RS'
GROUP BY mi.faixa_idh
ORDER BY mi.faixa_idh;
""".strip(),
        difficulty="difícil",
        tags=["socioeconômico", "IDH", "morte", "CTE", "comparação"],
    ),

    # ── DIFÍCIL: PANDEMIA ───────────────────────────────────────────────────

    NLSQLPair(
        question="Como a taxa de mortalidade hospitalar evoluiu durante a pandemia vs anos anteriores?",
        sql="""
SELECT EXTRACT(year FROM DT_INTER) AS ano,
       COUNT(*) AS internacoes,
       SUM(MORTE::INT) AS obitos,
       ROUND(100.0 * SUM(MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct,
       CASE
           WHEN EXTRACT(year FROM DT_INTER) IN (2020, 2021) THEN 'pandemia'
           ELSE 'pre_pandemia'
       END AS periodo
FROM internacoes
WHERE DT_INTER IS NOT NULL
GROUP BY ano
ORDER BY ano;
""".strip(),
        difficulty="difícil",
        tags=["pandemia", "série temporal", "morte", "CASE WHEN"],
    ),
    NLSQLPair(
        question="Qual o aumento percentual no custo médio por internação durante a pandemia?",
        sql="""
WITH custo_por_periodo AS (
    SELECT CASE
               WHEN EXTRACT(year FROM DT_INTER) IN (2020, 2021) THEN 'pandemia'
               ELSE 'pre_pandemia'
           END AS periodo,
           AVG(VAL_TOT) AS custo_medio
    FROM internacoes
    WHERE DT_INTER IS NOT NULL
      AND EXTRACT(year FROM DT_INTER) BETWEEN 2016 AND 2021
    GROUP BY periodo
)
SELECT
    MAX(CASE WHEN periodo = 'pre_pandemia' THEN custo_medio END) AS custo_pre_pandemia,
    MAX(CASE WHEN periodo = 'pandemia' THEN custo_medio END) AS custo_pandemia,
    ROUND(100.0 * (MAX(CASE WHEN periodo = 'pandemia' THEN custo_medio END)
                 - MAX(CASE WHEN periodo = 'pre_pandemia' THEN custo_medio END))
          / MAX(CASE WHEN periodo = 'pre_pandemia' THEN custo_medio END), 2)
          AS aumento_percentual
FROM custo_por_periodo;
""".strip(),
        difficulty="difícil",
        tags=["pandemia", "custo", "CTE", "comparação percentual"],
    ),

    # ── DIFÍCIL: COMPARAÇÃO RS vs MA ────────────────────────────────────────

    NLSQLPair(
        question="Compare o perfil de internações entre RS e MA: volume, custo médio e mortalidade",
        sql="""
SELECT m.estado,
       COUNT(*) AS total_internacoes,
       ROUND(AVG(i.VAL_TOT), 2) AS custo_medio,
       ROUND(AVG(i.DIAS_PERM), 2) AS media_dias_perm,
       SUM(i.MORTE::INT) AS total_obitos,
       ROUND(100.0 * SUM(i.MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
GROUP BY m.estado
ORDER BY m.estado;
""".strip(),
        difficulty="difícil",
        tags=["comparação geográfica", "RS", "MA", "perfil completo"],
    ),
    NLSQLPair(
        question="Evolução anual do custo total do SUS por estado",
        sql="""
SELECT EXTRACT(year FROM i.DT_INTER) AS ano,
       m.estado,
       COUNT(*) AS internacoes,
       ROUND(SUM(i.VAL_TOT) / 1e6, 2) AS custo_milhoes_reais
FROM internacoes i
JOIN municipios m ON i.MUNIC_RES = m.codigo_6d
WHERE i.DT_INTER IS NOT NULL
GROUP BY ano, m.estado
ORDER BY ano, m.estado;
""".strip(),
        difficulty="difícil",
        tags=["custo", "série temporal", "comparação geográfica"],
    ),

    # ── DIFÍCIL: PROCEDIMENTOS ──────────────────────────────────────────────

    NLSQLPair(
        question="Quais os 10 procedimentos mais realizados em internações clínicas?",
        sql="""
SELECT p.NOME_PROC AS procedimento,
       COUNT(*) AS total_realizados
FROM atendimentos a
JOIN procedimentos p ON a.PROC_REA = p.PROC_REA
JOIN internacoes i ON a.N_AIH = i.N_AIH
JOIN especialidade e ON i.ESPEC = e.ESPEC
WHERE e.ESPEC = 3
GROUP BY p.NOME_PROC
ORDER BY total_realizados DESC
LIMIT 10;
""".strip(),
        difficulty="difícil",
        tags=["procedimentos", "especialidade", "ranking", "join múltiplo"],
    ),

    # ── DIFÍCIL: ANÁLISE DE EQUIDADE ────────────────────────────────────────

    NLSQLPair(
        question="Existe diferença na taxa de mortalidade entre pacientes brancos e pardos em internações clínicas?",
        sql="""
SELECT r.DESCRICAO AS raca_cor,
       COUNT(*) AS internacoes,
       SUM(i.MORTE::INT) AS obitos,
       ROUND(100.0 * SUM(i.MORTE::INT) / COUNT(*), 2) AS taxa_mortalidade_pct,
       ROUND(AVG(i.DIAS_PERM), 2) AS media_dias_perm
FROM internacoes i
JOIN raca_cor r ON i.RACA_COR = r.RACA_COR
WHERE i.ESPEC = 3
  AND i.RACA_COR IN (1, 3)
GROUP BY r.DESCRICAO
ORDER BY taxa_mortalidade_pct DESC;
""".strip(),
        difficulty="difícil",
        tags=["raça", "equidade", "morte", "especialidade"],
    ),
    NLSQLPair(
        question="Qual a escolaridade mais frequente entre pacientes internados com AVC?",
        sql="""
SELECT inst.DESCRICAO AS nivel_escolaridade,
       COUNT(*) AS total
FROM internacoes i
JOIN instrucao inst ON i.INSTRU = inst.INSTRU
WHERE i.DIAG_PRINC LIKE 'I6%'
  AND i.INSTRU != 0
GROUP BY inst.DESCRICAO
ORDER BY total DESC;
""".strip(),
        difficulty="difícil",
        tags=["escolaridade", "diagnóstico", "AVC", "CID"],
    ),
]


def get_few_shot_examples(question: str, n: int = 3) -> list[NLSQLPair]:
    """
    Seleciona os exemplos mais relevantes por matching de palavras-chave.
    Para produção, substituir por busca por embedding semântico.
    """
    q_lower = question.lower()
    scored: list[tuple[int, NLSQLPair]] = []

    for pair in NL_SQL_PAIRS:
        score = sum(
            1 for word in pair.question.lower().split()
            if len(word) > 3 and word in q_lower
        )
        scored.append((score, pair))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:n]]


def format_few_shot(examples: list[NLSQLPair]) -> str:
    """Formata exemplos para inserção no prompt."""
    lines = []
    for i, ex in enumerate(examples, 1):
        lines.append(f"Exemplo {i}:")
        lines.append(f"Pergunta: {ex.question}")
        lines.append(f"SQL:\n{ex.sql}")
        lines.append("")
    return "\n".join(lines)
