import streamlit as st
import pandas as pd

from app.utils.ui import render_sidebar_branding

# 1) set_page_config sempre no topo (primeiro st.* do arquivo)
st.set_page_config(
    page_title="RF Finance - Início",
    layout="wide",
)

# 2) Sidebar: logo + título
render_sidebar_branding()

# =========================
# HOME / HUB (Opção 1)
# =========================
st.title("RF Tecnology - Financeiro")
st.caption("Fluxo recomendado do fechamento mensal:")

st.markdown("""
### INSTRUÇÕES RÁPIDAS:
1. **Plano de Contas (Rubricas)** → cadastre O PLANO DE CONTAS oficial da RF Consultores.  
2. **Aplicar Histórico** → processe o mês (ex.: Wiew 01-2026) e gere o export.  
3. **Resolver Exceções (Menu)** → selecione as rubricas no dropdown (Caixa de seleção) e salve o lote.  
4. Volte em **Aplicar Histórico** e **reprocesse 1 vez** para aplicar as novas regras.
""")

st.subheader("Atalhos")
c1, c2, c3 = st.columns(3)

with c1:
    st.page_link("pages/5_Plano_de_Contas_Rubricas.py", label="Plano de Contas")
with c2:
    st.page_link("pages/3_Aplicar_Historico.py", label="Aplicar Histórico")
with c3:
    st.page_link("pages/6_Resolver_Excecoes_Menu.py", label="Resolver Exceções (Menu)")

st.divider()

# =========================
# MVP-0 ORIGINAL (mantido)
# =========================
with st.expander("🔎 Ferramenta técnica (Upload + Validação + Competência)", expanded=False):
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

    st.caption(f"Competência: {COMPETENCIA_FIELD} | Valor padrão: {VALUE_FIELD}")

    uploaded = st.file_uploader("Faça upload do Excel do sistema (.xlsx)", type=["xlsx"])
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
        index=sheet_names.index(SHEET_DEFAULT) if SHEET_DEFAULT in sheet_names else 0
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
        st.warning("Dica: verifique se você selecionou a aba correta ou se o layout do export mudou.")
        st.stop()
    st.success("Colunas obrigatórias OK ✅")

    st.subheader("2) Derivar competência (COMPETENCIA_MES) por DATA_EMISSAO")
    df[COMPETENCIA_FIELD] = pd.to_datetime(df[COMPETENCIA_FIELD], errors="coerce")
    null_dates = int(df[COMPETENCIA_FIELD].isna().sum())
    if null_dates > 0:
        st.warning(f"{null_dates} linhas sem DATA_EMISSAO válida (na amostra de {NROWS_SAMPLE}).")

    df["COMPETENCIA_MES"] = df[COMPETENCIA_FIELD].dt.to_period("M").astype(str)

    df[VALUE_FIELD] = pd.to_numeric(df[VALUE_FIELD], errors="coerce")
    st.metric("Linhas (amostra)", int(len(df)))
    st.metric("Competências distintas (amostra)", int(df["COMPETENCIA_MES"].nunique()))
    st.metric("Soma VALOR_TITULO (amostra)", float(df[VALUE_FIELD].fillna(0).sum()))

    st.subheader("3) Prévia (amostra)")
    cols_preview = [
        "COMPETENCIA_MES",
        "TIPO",
        "SITUACAO",
        "NOME_CONTA",
        "NOME_PESSOA",
        "NOME_CENTRO_CUSTO",
        "VALOR_TITULO",
        "DATA_EMISSAO",
        "CODIGO_INTERNO_TITULO",
        "NUMERO_TITULO",
        "PARCELA",
    ]
    cols_preview = [c for c in cols_preview if c in df.columns]
    st.dataframe(df[cols_preview].head(50), use_container_width=True)

    with st.expander("Diagnóstico técnico (tipos de dados e colunas)"):
        st.write("Colunas encontradas:")
        st.code("\n".join(df.columns.astype(str).tolist()))
        st.write("Tipos de dados (amostra):")
        st.dataframe(df.dtypes.astype(str), use_container_width=True)