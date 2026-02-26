import streamlit as st

st.title("Treinar Histórico")

from app.utils.ui import render_sidebar_branding
render_sidebar_branding()

import pandas as pd

from app.db.repositories.rubricas_repo import (
    ensure_rubricas_schema,
    list_rubricas,
    upsert_rubrica,
    set_rubrica_ativo,
    delete_rubrica,
)

st.title("Plano de Contas — Cadastro de Rubricas")

ensure_rubricas_schema()

with st.expander("➕ Cadastrar / Atualizar rubrica", expanded=True):
    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])

    with c1:
        rubrica = st.text_input("Rubrica (nome bonito)", placeholder="Ex.: Despesas com veículos")
    with c2:
        grupo = st.selectbox("Grupo", ["DESPESA", "RECEITA"])
    with c3:
        natureza = st.selectbox("Natureza", ["OPERACIONAL", "NAO_OPERACIONAL"])
    with c4:
        ativo = st.checkbox("Ativa", value=True)

    if st.button("Salvar rubrica"):
        try:
            upsert_rubrica(
                rubrica_display=rubrica,
                grupo=grupo,
                natureza=natureza,
                ativo=1 if ativo else 0,
            )
            st.success("✅ Rubrica salva/atualizada.")
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

st.divider()
st.subheader("Rubricas cadastradas")

data = list_rubricas(only_active=False)
if not data:
    st.info("Nenhuma rubrica cadastrada ainda.")
    st.stop()

df = pd.DataFrame(data)
df["ativo"] = df["ativo"].astype(int)

st.dataframe(df, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Ações rápidas")

ids = df["id"].tolist()
colA, colB, colC = st.columns(3)

with colA:
    rid = st.selectbox("Rubrica (ID)", ids)

with colB:
    novo_ativo = st.selectbox("Definir como", [1, 0], format_func=lambda x: "Ativa" if x == 1 else "Inativa")
    if st.button("Aplicar status"):
        set_rubrica_ativo(rid, novo_ativo)
        st.success("✅ Status atualizado.")

with colC:
    if st.button("Excluir rubrica (cuidado)"):
        delete_rubrica(rid)
        st.warning("🗑️ Rubrica excluída.")