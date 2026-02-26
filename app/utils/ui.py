
from pathlib import Path
import streamlit as st

def render_sidebar_branding():
    # Caminho da logo
    logo_path = Path("app/assets/LOGO RF branca.png")

    # Espaçamento no topo (opcional, pode remover se quiser colado)
    st.sidebar.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    # Centralizar a logo usando colunas no sidebar
    c1, c2, c3 = st.sidebar.columns([1, 2, 1])
    with c2:
        if logo_path.exists():
            st.image(str(logo_path), width=180)  # "tamanho médio" (ajuste fino se quiser 160/200)
        else:
            st.warning("Logo não encontrada em app/assets/LOGO RF branca.png")

    # Título centralizado abaixo
    st.sidebar.markdown(
        "<div style='text-align:center; font-weight:700; font-size:18px; margin-top:6px;'>RF TECNOLOGY</div>",
        unsafe_allow_html=True
    )

    # Linha sutil separadora (opcional – se não quiser, apague)
    st.sidebar.markdown("<hr style='margin: 12px 0; opacity:0.25;'>", unsafe_allow_html=True)