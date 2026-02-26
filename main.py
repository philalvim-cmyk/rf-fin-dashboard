
import streamlit as st
import pandas as pd

from app.utils.auth import require_login
from app.utils.ui import render_sidebar_branding

# =====================================================
# CONFIGURAÇÃO GLOBAL (OBRIGATORIAMENTE PRIMEIRO)
# =====================================================
st.set_page_config(
    page_title="RF Technology - SNIPER",
    layout="wide",
)

require_login()

from pathlib import Path

LOGO_PATH = Path("assets/LOGO_TDC.png")

if LOGO_PATH.exists():
    st.image(str(LOGO_PATH), width=180)
# =====================================================
# SIDEBAR (BRANDING SEGURO)
# =====================================================
render_sidebar_branding()



# =====================================================
# HOME
# =====================================================
st.header("RF Technology - 🎯SNIPER🎯")
st.caption("Fluxo recomendado do fechamento mensal:")

st.subheader("""♟️INSTRUÇÕES:
1. **Plano de Contas (Rubricas)** → cadastre o PLANO DE CONTAS oficial da RF Consultores.
2. **Aplicar Histórico** → processe o mês (ex.: View 01-2026) e gere o export.
3. **Resolver Exceções (Menu)** → selecione as rubricas no dropdown e salve o lote.
4. Volte em **Aplicar Histórico** e reprocesse 1 vez para aplicar as novas regras.
""")

st.divider()

# =====================================================
# MVP-0 ORIGINAL (MANTIDO)
# =====================================================
with st.expander(
    "🔎 Ferramenta técnica (Upload + Validação + Competência)",
    expanded=False
):
    st.title("RF - Análises (Upload + Validação + Competência)")

    SHEET_DEFAULT = "VW_CTO_FINAN"
    COMPETENCIA_FIELD = "DATA_EMISSAO"
    VALUE_FIELD = "VALOR_TITULO"

    REQUIRED_COLUMNS = [
        "CNPJ_EMPRESA",
        "TIPO",
        "CODIGO_INTERNO_TITULO",
        "NUMERO_TITULO",
        "PARCELA",
        "VALOR_TITULO",
        "DATA_EMISSAO",
        "SITUACAO",
        "NOME_CONTA",
        "NOME_PESSOA",
        "NOME_CENTRO_CUSTO",
    ]

    st.caption(
        f"Competência: {COMPETENCIA_FIELD} | "
        f"Valor padrão: {VALUE_FIELD}"
    )

    uploaded = st.file_uploader(
        "Faça upload do Excel do sistema (.xlsx)",
        type=["xlsx"]
    )

    if uploaded is None:
        st.info("Envie um arquivo para começar.")
        st.stop()

    st.subheader("0) Seleção de aba")

    try:
        xls = pd.ExcelFile(uploaded, engine="openpyxl")
        sheet_names = xls.sheet_names
    except Exception as e:
        st.error(f"Não foi possível abrir o arquivo Excel. Erro: {e}")
        st.stop()

    sheet_selected = st.selectbox(
        "Escolha a aba que contém os lançamentos:",
        options=sheet_names,
        index=sheet_names.index(SHEET_DEFAULT)
        if SHEET_DEFAULT in sheet_names else 0
    )

    st.info(f"Aba selecionada: {sheet_selected}")

    NROWS_SAMPLE = 2000

    try:
        df = pd.read_excel(
            uploaded,
            sheet_name=sheet_selected,
            engine="openpyxl",
            nrows=NROWS_SAMPLE
        )
    except Exception as e:
        st.error(f"Falha ao ler a aba '{sheet_selected}'. Erro: {e}")
        st.stop()

    st.subheader("1) Validação de colunas (amostra)")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        st.error("Colunas obrigatórias ausentes nesta aba:")
        st.code("\n".join(missing))
        st.stop()

    st.success("Colunas obrigatórias OK ✅")

    st.subheader("2) Derivar competência")

    df[COMPETENCIA_FIELD] = pd.to_datetime(
        df[COMPETENCIA_FIELD],
        errors="coerce"
    )

    df["COMPETENCIA_MES"] = (
        df[COMPETENCIA_FIELD]
        .dt.to_period("M")
        .astype(str)
    )

    df[VALUE_FIELD] = pd.to_numeric(
        df[VALUE_FIELD],
        errors="coerce"
    )

    st.metric("Linhas (amostra)", len(df))
    st.metric(
        "Competências distintas",
        df["COMPETENCIA_MES"].nunique()
    )