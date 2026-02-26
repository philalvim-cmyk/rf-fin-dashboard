import streamlit as st

st.set_page_config(page_title="Treinar Histórico", layout="wide")

from pathlib import Path
import pandas as pd
import datetime as dt

from app.utils.strings import normalize_text
from app.db.repositories.history_repo import bulk_upsert_history
from app.utils.ui import render_sidebar_branding

render_sidebar_branding()

st.title("Treinar Histórico — Base classificada")

st.markdown(
    """
Esta página treina o **histórico** (tabela SQLite) usando um arquivo base que já contém a
coluna de **CLASSIFICAÇÃO RF** preenchida manualmente (ex.: base 2025).
"""
)

uploaded = st.file_uploader("Upload do Excel base classificado (ex.: 2025 classificado)", type=["xlsx"])

if uploaded is None:
    st.info("Envie o arquivo base classificado para iniciar o treino.")
    st.stop()

file_bytes = uploaded.getvalue()
tmp_dir = Path("data/processed")
tmp_dir.mkdir(parents=True, exist_ok=True)
tmp_path = tmp_dir / f"treino_base_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
tmp_path.write_bytes(file_bytes)

st.subheader("0) Seleção de aba e colunas")
xls = pd.ExcelFile(tmp_path, engine="openpyxl")
sheet = st.selectbox("Aba de dados (base)", xls.sheet_names, index=0)

df_preview = pd.read_excel(tmp_path, sheet_name=sheet, engine="openpyxl", nrows=50)
st.caption("Prévia (primeiras 50 linhas):")
st.dataframe(df_preview, use_container_width=True)

# Tentativa robusta de detectar a coluna de classificação (base pode ter nome com/sem acento)
possible_class_cols = [
    "CLASSIFICAÇÃO RF",
    "CLASSIFICACAO RF",
    "CLASSIFICACAO_RF",
    "CLASSIFICAÇÃO_RF",
    "CLASSIFICACAO",
    "CLASSIFICAÇÃO",
]
class_col = None
for c in possible_class_cols:
    if c in df_preview.columns:
        class_col = c
        break

if class_col is None:
    st.error(
        "Não encontrei a coluna de classificação na base.\n"
        "Colunas esperadas (alguma delas):\n"
        + "\n".join(possible_class_cols)
    )
    st.stop()

st.success(f"Coluna de classificação detectada: **{class_col}**")

# Campos de chave (sempre os mesmos do projeto)
required_key_cols = ["NOME_CONTA", "NOME_PESSOA", "NOME_CENTRO_CUSTO"]
missing = [c for c in required_key_cols if c not in df_preview.columns]
if missing:
    st.error(f"Colunas-chave ausentes na aba: {missing}")
    st.stop()

limit = st.number_input("Limite de linhas para treino (0 = sem limite)", min_value=0, value=0, step=1000)

st.subheader("1) Treinar / Atualizar histórico")
if st.button("Treinar histórico agora"):
    with st.spinner("Lendo base e gravando no SQLite..."):
        df = pd.read_excel(
            tmp_path,
            sheet_name=sheet,
            engine="openpyxl",
            nrows=None if int(limit) == 0 else int(limit),
        )

        # Normalização defensiva
        for c in required_key_cols + [class_col]:
            if c not in df.columns:
                st.error(f"Coluna obrigatória ausente: {c}")
                st.stop()

        df[class_col] = df[class_col].astype(str).fillna("").str.strip()
        df = df[df[class_col] != ""].copy()

        if df.empty:
            st.warning("Nenhuma linha com classificação preenchida encontrada.")
            st.stop()

        # Normaliza chaves
        df["NOME_CONTA_N"] = df["NOME_CONTA"].fillna("").apply(normalize_text)
        df["NOME_PESSOA_N"] = df["NOME_PESSOA"].fillna("").apply(normalize_text)
        df["NOME_CC_N"] = df["NOME_CENTRO_CUSTO"].fillna("").apply(normalize_text)

        # Normaliza classificação (pode manter “bonita” no futuro; aqui armazenamos como está)
        # Se quiser padronizar no DB, pode trocar para normalize_text, mas não é obrigatório.
        df["CLASS_RF"] = df[class_col].astype(str).str.strip()

        # Agrupa para evitar gravação repetida (mesma chave -> 1 registro)
        grp = (
            df.groupby(["NOME_CONTA_N", "NOME_PESSOA_N", "NOME_CC_N", "CLASS_RF"], dropna=False)
            .size()
            .reset_index(name="qtd")
        )

        now = dt.datetime.now().isoformat(timespec="seconds")
        records = []
        for _, r in grp.iterrows():
            records.append(
                {
                    "key_type": "CONTA+PESSOA+CC",
                    "nome_conta": r["NOME_CONTA_N"],
                    "nome_pessoa": r["NOME_PESSOA_N"],
                    "nome_centro_custo": r["NOME_CC_N"],
                    "classificacao_rf": r["CLASS_RF"],
                    "hit_count": int(r["qtd"]),
                    "last_used_at": now,
                }
            )

        affected = bulk_upsert_history(records)
        st.success(f"✅ Treino concluído! Registros gravados/atualizados: {affected}")
        st.info("Agora vá para 'Aplicar Histórico' e processe o mês (ex.: 2026-01).")