import streamlit as st

st.title("Treinar Histórico")

from app.utils.ui import render_sidebar_branding
render_sidebar_branding()

import pandas as pd
from pathlib import Path
import time

from app.config.constants import SHEET_DEFAULT, COLUMNS
from app.services.ingest import stream_filter_by_competencia
from app.services.transform import add_competencia, normalize_keys
from app.services.classify import apply_classification
from app.services.aggregate import consolidate_dinamica
from app.services.export import export_excel
from app.utils.hashing import sha256_bytes

st.title("Aplicar Histórico → Classificar mês e gerar DINÂMICA")

uploaded = st.file_uploader("Upload do Excel do sistema (ex.: Wiew 01-2026)", type=["xlsx"])
if uploaded is None:
    st.info("Envie o arquivo do sistema para processar o mês.")
    st.stop()

file_bytes = uploaded.getvalue()
file_hash = sha256_bytes(file_bytes)

# salvar temporário (streaming precisa de caminho)
tmp_dir = Path("data/processed")
tmp_dir.mkdir(parents=True, exist_ok=True)
tmp_path = tmp_dir / f"mes_{file_hash}.xlsx"
tmp_path.write_bytes(file_bytes)

# escolher aba
xls = pd.ExcelFile(tmp_path, engine="openpyxl")
sheet = st.selectbox(
    "Aba de dados",
    xls.sheet_names,
    index=xls.sheet_names.index(SHEET_DEFAULT) if SHEET_DEFAULT in xls.sheet_names else 0
)

competencia = st.text_input("Competência (YYYY-MM) baseada em DATA_EMISSAO", value="2026-01")


# -----------------------------------------------------------------------------
# Helpers cirúrgicos de FORMATAÇÃO (somente UI, não altera o df real)
# -----------------------------------------------------------------------------
def fmt_br(x) -> str:
    """Formata números no padrão brasileiro: 1.234.567,89 (2 casas)."""
    if x is None:
        return ""
    try:
        s = f"{float(x):,.2f}"  # 1,234,567.89
        return s.replace(",", "X").replace(".", ",").replace("X", ".")  # 1.234.567,89
    except Exception:
        return str(x)


def styler_br(df: pd.DataFrame, cols: list[str]):
    """
    Retorna um Styler que formata colunas numéricas como BR.
    Não altera df (somente visualização).
    """
    fmt_map = {}
    for c in cols:
        if c in df.columns:
            fmt_map[c] = lambda v: fmt_br(v)
    return df.style.format(fmt_map, na_rep="")


def _progress(bar, status, pct: int, msg: str):
    """Atualiza barra e texto com percent numérico 0-100."""
    pct = max(0, min(100, int(pct)))
    bar.progress(pct, text=f"{pct}% — {msg}")
    status.info(msg)


# -----------------------------------------------------------------------------
# Processamento
# -----------------------------------------------------------------------------
if st.button("Processar competência e classificar"):
    bar = st.progress(0, text="0% — Iniciando...")
    status = st.empty()
    t0 = time.perf_counter()

    try:
        with st.spinner("Processando e aplicando histórico..."):
            _progress(bar, status, 5, "Preparando ambiente (arquivo e parâmetros)")

            _progress(bar, status, 15, f"Lendo Excel (streaming) e filtrando competência {competencia}")
            df = stream_filter_by_competencia(tmp_path, sheet_name=sheet, competencia=competencia, columns=COLUMNS)
            st.write(f"Linhas encontradas para {competencia}: {len(df)}")

            _progress(bar, status, 45, "Transformando (competência + normalização de chaves)")
            df = add_competencia(df, competencia_field="DATA_EMISSAO")
            df = normalize_keys(df)

            _progress(bar, status, 65, "Aplicando classificação (histórico SQLite)")
            df = apply_classification(df)

            # métricas de classificação
            total = len(df)
            nao = int((df["CLASSIFICACAO_RF"] == "NAO_CLASSIFICADO").sum()) if total else 0
            auto = total - nao
            pct_auto = (auto / total * 100) if total else 0.0

            st.success(
                f"Classificação concluída ✅\n"
                f"Automático: {pct_auto:.1f}%\n"
                f"Não classificado: {nao}"
            )

            _progress(bar, status, 82, "Consolidando DINÂMICA")
            df_con = consolidate_dinamica(df, value_field="VALOR_TITULO")

            # -----------------------------
            # EXIBIÇÃO BR (somente UI)
            # -----------------------------
            st.subheader("DINÂMICA (Consolidado)")
            # df_con tem coluna VALOR (numérica). Mostramos com Styler BR.
            st.dataframe(styler_br(df_con, ["VALOR"]), use_container_width=True)

            st.subheader("Não classificados (top 100)")
            df_nc = df[df["CLASSIFICACAO_RF"] == "NAO_CLASSIFICADO"].head(100)
            # Mostramos VALOR_TITULO em BR (sem alterar o df real)
            st.dataframe(styler_br(df_nc, ["VALOR_TITULO"]), use_container_width=True)

            _progress(bar, status, 92, "Gerando export Excel")
            out_path = export_excel(df_con, df, out_dir="data/exports")

            st.success(f"Export gerado: {out_path}")

            with open(out_path, "rb") as f:
                st.download_button(
                    "Baixar Excel (DINÂMICA + Detalhado)",
                    data=f,
                    file_name=out_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            elapsed = time.perf_counter() - t0
            bar.progress(100, text=f"100% — Concluído ✅ (tempo total: {elapsed:.1f}s)")
            status.success("Processo finalizado com sucesso.")

    except Exception as e:
        bar.progress(100, text="100% — Falha ❌ (veja detalhes abaixo)")
        status.error("Ocorreu um erro durante o processamento.")
        st.exception(e)