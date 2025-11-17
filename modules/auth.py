"""
Módulo de autenticação do sistema CRED-CANCEL v3.0
"""

import streamlit as st
import hashlib
from .config import SENHA_ACESSO


def check_password():
    """
    Verifica autenticação do usuário.

    Returns:
        bool: True se autenticado, False caso contrário
    """
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        _render_login_page()
        st.stop()

    return True


def _render_login_page():
    """Renderiza a página de login."""
    st.markdown(
        "<div style='text-align: center; padding: 50px;'>"
        "<h1>🔐 CRED-CANCEL v3.0</h1>"
        "<h3>Sistema Integrado de Análise Fiscal</h3>"
        "<p>Receita Estadual de Santa Catarina - SEF/SC</p>"
        "</div>",
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("---")
        st.markdown("### Acesso Restrito")
        st.caption("Digite suas credenciais para acessar o sistema")

        senha_input = st.text_input(
            "Senha:",
            type="password",
            key="pwd_input",
            placeholder="Digite a senha de acesso"
        )

        col_btn1, col_btn2 = st.columns([1, 1])

        with col_btn1:
            if st.button("🔓 Entrar", use_container_width=True, type="primary"):
                if senha_input == SENHA_ACESSO:
                    st.session_state.authenticated = True
                    st.success("✅ Autenticação bem-sucedida!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta. Tente novamente.")

        with col_btn2:
            if st.button("ℹ️ Ajuda", use_container_width=True):
                st.info(
                    "**Sistema de Autenticação**\n\n"
                    "Entre em contato com a equipe responsável "
                    "para obter as credenciais de acesso.\n\n"
                    "**Contato:** AFRE Tiago Severo"
                )

        st.markdown("---")
        st.caption("© 2025 SEF/SC - Todos os direitos reservados")


def logout():
    """Realiza logout do sistema."""
    st.session_state.authenticated = False
    st.rerun()


def is_authenticated():
    """
    Verifica se o usuário está autenticado.

    Returns:
        bool: True se autenticado
    """
    return st.session_state.get('authenticated', False)
