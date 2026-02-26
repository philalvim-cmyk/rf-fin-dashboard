import streamlit as st

def require_login():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return

    st.title("🔐 Acesso Restrito")

    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if username == "cliente" and password == "1234":
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos")

    # ⛔ PARA A EXECUÇÃO TOTAL DA PÁGINA
    st.stop()