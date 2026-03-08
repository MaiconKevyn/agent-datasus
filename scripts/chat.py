#!/usr/bin/env python3
"""
Interface de chat Streamlit para o Text-to-SQL DATASUS.

Uso:
    streamlit run scripts/chat.py

Requer API rodando em localhost:8000 OU carrega o pipeline diretamente
(modo standalone, sem API).

O modo é detectado automaticamente: tenta a API, cai para standalone se
não estiver disponível.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="DATASUS Text-to-SQL",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Backend: API ou standalone
# ---------------------------------------------------------------------------

API_URL = "http://localhost:8000"


def _call_api(question: str) -> dict:
    import urllib.request
    import json

    payload = json.dumps({"question": question}).encode()
    req = urllib.request.Request(
        f"{API_URL}/query",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read())


def _call_standalone(question: str) -> dict:
    """Chama o pipeline diretamente sem a API."""
    from src.text2sql.pipeline import Text2SQLPipeline
    from src.text2sql.schema_linker import build_schema_index
    from src.text2sql.few_shot_store import build_few_shot_index

    if "pipeline" not in st.session_state:
        with st.spinner("Carregando pipeline..."):
            schema_store = build_schema_index()
            few_shot_store = build_few_shot_index()
            st.session_state.pipeline = Text2SQLPipeline(
                schema_store=schema_store,
                few_shot_store=few_shot_store,
            )

    result = st.session_state.pipeline.run(question)
    return {
        "success": result.success,
        "question": result.question,
        "sql": result.sql,
        "columns": result.columns,
        "rows": [list(r) for r in result.rows],
        "row_count": len(result.rows),
        "repair_attempts": result.repair_attempts,
        "latency_ms": result.latency_ms,
        "tokens_used": result.tokens_used,
        "tables_used": result.tables_used,
        "schema_tables_selected": result.schema_tables_selected,
        "error": result.error,
    }


def run_query(question: str) -> dict:
    """Tenta API; cai para standalone se não disponível."""
    try:
        return _call_api(question)
    except Exception:
        return _call_standalone(question)


def check_api() -> bool:
    try:
        import urllib.request
        urllib.request.urlopen(f"{API_URL}/health", timeout=2)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if "history" not in st.session_state:
    st.session_state.history = []   # lista de dicts com question + result

if "api_available" not in st.session_state:
    st.session_state.api_available = check_api()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🏥 DATASUS Text-to-SQL")
    st.caption("SIH-RD | RS + MA | 2008–2023")

    api_badge = "🟢 API" if st.session_state.api_available else "🟡 Standalone"
    st.caption(f"Modo: {api_badge}")

    st.divider()

    st.subheader("Exemplos de perguntas")
    examples = [
        ("📊 Simples", [
            "Quantas internações ocorreram em 2022?",
            "Qual o custo total pago pelo SUS em internações?",
            "Quantos óbitos foram registrados no total?",
        ]),
        ("🔍 Médio", [
            "Quais os 10 diagnósticos mais frequentes?",
            "Qual a taxa de mortalidade por especialidade?",
            "Quantas internações de mulheres ocorreram em 2023?",
            "Quais os 5 hospitais com maior número de internações no MA?",
        ]),
        ("🧩 Difícil", [
            "Compare o perfil de internações entre RS e MA",
            "Como a mortalidade evoluiu durante a pandemia?",
            "Qual a relação entre IDH municipal e mortalidade hospitalar?",
            "Existe diferença na mortalidade entre brancos e pardos em internações clínicas?",
        ]),
    ]

    for label, questions in examples:
        with st.expander(label, expanded=False):
            for q in questions:
                if st.button(q, key=f"ex_{q[:30]}", use_container_width=True):
                    st.session_state.pending_question = q

    st.divider()

    if st.button("🗑️ Limpar histórico", use_container_width=True):
        st.session_state.history = []
        st.rerun()

    if st.session_state.history:
        total = len(st.session_state.history)
        ok = sum(1 for h in st.session_state.history if h["result"]["success"])
        avg_ms = int(sum(h["result"]["latency_ms"] for h in st.session_state.history) / total)
        st.caption(f"Sessão: {ok}/{total} OK | ~{avg_ms}ms")

# ---------------------------------------------------------------------------
# Área principal
# ---------------------------------------------------------------------------

st.header("Pergunte sobre internações hospitalares no SUS")
st.caption("Base: SIH-RD DATASUS — RS e MA — 18,4 milhões de internações — 2008 a 2023")

# Input de pergunta
pending = st.session_state.pop("pending_question", None)
question = st.text_input(
    "Sua pergunta:",
    value=pending or "",
    placeholder="Ex: Quais os 5 diagnósticos mais frequentes no Maranhão?",
    key="question_input",
)

col1, col2 = st.columns([1, 5])
with col1:
    submit = st.button("Enviar", type="primary", use_container_width=True)
with col2:
    st.caption("Pressione Enter ou clique em Enviar")

# Processa pergunta
if submit and question.strip():
    with st.spinner("Gerando SQL e consultando o banco..."):
        t0 = time.time()
        result = run_query(question.strip())
        elapsed = time.time() - t0

    st.session_state.history.insert(0, {"question": question.strip(), "result": result})

# ---------------------------------------------------------------------------
# Exibição do histórico
# ---------------------------------------------------------------------------

for i, entry in enumerate(st.session_state.history):
    q = entry["question"]
    r = entry["result"]

    with st.container():
        # Cabeçalho da pergunta
        status_icon = "✅" if r["success"] else "❌"
        st.markdown(f"**{status_icon} {q}**")

        if r["success"]:
            # Métricas em linha
            mcols = st.columns(4)
            mcols[0].metric("Linhas", r["row_count"])
            mcols[1].metric("Latência", f"{r['latency_ms']}ms")
            mcols[2].metric("Tokens", r["tokens_used"].get("total", 0))
            mcols[3].metric("Repairs", r["repair_attempts"])

            # Resultado em tabela
            if r["rows"]:
                import pandas as pd
                df = pd.DataFrame(r["rows"], columns=r["columns"] if r["columns"] else None)
                st.dataframe(df, use_container_width=True, height=min(300, 40 + len(df) * 35))
            else:
                st.info("Query executou com sucesso mas retornou 0 linhas.")

            # SQL e detalhes (expansíveis)
            with st.expander("SQL gerado", expanded=(i == 0)):
                st.code(r["sql"], language="sql")

            with st.expander("Detalhes técnicos", expanded=False):
                dcols = st.columns(2)
                dcols[0].write(f"**Tabelas na query:** {', '.join(r['tables_used']) or '—'}")
                dcols[1].write(f"**Schema selecionado:** {', '.join(r['schema_tables_selected']) or '—'}")
        else:
            st.error(f"Erro: {r.get('error', 'desconhecido')}")
            if r.get("sql"):
                with st.expander("SQL gerado (com erro)", expanded=False):
                    st.code(r["sql"], language="sql")

        st.divider()
