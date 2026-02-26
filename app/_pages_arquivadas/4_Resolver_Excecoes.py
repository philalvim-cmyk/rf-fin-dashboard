import streamlit as st
import pandas as pd
from pathlib import Path
import datetime as dt

from app.utils.strings import normalize_text
from app.db.repositories.history_repo import bulk_upsert_history

st.set_page_config(page_title="Resolver Exceções (Lote)", layout="wide")
st.title("Resolver Exceções (Lote) — classificar tudo e salvar uma vez")

exports_dir = Path("data/exports")
exports = sorted(exports_dir.glob("export_rf_*.xlsx"))
if not exports:
    st.warning("Nenhum export encontrado em data/exports. Rode 'Aplicar Histórico' primeiro.")
    st.stop()

last_export = exports[-1]
st.caption(f"Usando último export: {last_export.name}")

df = pd.read_excel(last_export, sheet_name="DETALHADO_CLASSIFICADO", engine="openpyxl")

# garante colunas normalizadas
for col_src, col_n in [
    ("NOME_CONTA", "NOME_CONTA_N"),
    ("NOME_PESSOA", "NOME_PESSOA_N"),
    ("NOME_CENTRO_CUSTO", "NOME_CC_N"),
]:
    if col_n not in df.columns:
        df[col_n] = df[col_src].apply(normalize_text)

df_nc = df[df.get("CLASSIFICACAO_RF", "") == "NAO_CLASSIFICADO"].copy()

st.write(f"Total linhas no export: {len(df)}")
st.write(f"Total NAO_CLASSIFICADO: {len(df_nc)}")

if df_nc.empty:
    st.success("Sem exceções!")
    st.stop()

grp = (
    df_nc.groupby(["NOME_CONTA_N", "NOME_PESSOA_N", "NOME_CC_N"], dropna=False)
         .agg(qtd=("VALOR_TITULO", "size"), valor=("VALOR_TITULO", "sum"))
         .reset_index()
         .sort_values(["valor", "qtd"], ascending=False)
)

# coluna para o usuário preencher
if "NOVA_CLASSIFICACAO_RF" not in grp.columns:
    grp["NOVA_CLASSIFICACAO_RF"] = ""

st.info("Preencha a NOVA_CLASSIFICACAO_RF nas combinações que você quer ensinar. Depois clique em 'Salvar lote'.")

edited = st.data_editor(
    grp,
    use_container_width=True,
    num_rows="dynamic",
    hide_index=True,
)

col1, col2 = st.columns(2)

with col1:
    if st.button("Salvar lote no histórico (SQLite)"):
        now = dt.datetime.now().isoformat(timespec="seconds")

        to_save = edited[edited["NOVA_CLASSIFICACAO_RF"].astype(str).str.strip() != ""].copy()
        if to_save.empty:
            st.warning("Nada preenchido para salvar.")
            st.stop()

        records = []
        for _, r in to_save.iterrows():
            records.append({
                "key_type": "CONTA+PESSOA+CC",
                "nome_conta": r["NOME_CONTA_N"],
                "nome_pessoa": r["NOME_PESSOA_N"],
                "nome_centro_custo": r["NOME_CC_N"],
                "classificacao_rf": normalize_text(r["NOVA_CLASSIFICACAO_RF"]),
                "hit_count": int(r["qtd"]),
                "last_used_at": now
            })

        affected = bulk_upsert_history(records)
        st.success(f"✅ Lote salvo! Registros gravados/atualizados no histórico: {affected}")
        st.info("Agora volte em 'Aplicar Histórico' e reprocessa UMA vez (2026-01).")

with col2:
    st.write("Dica: comece pelas maiores por VALOR (já está ordenado).")