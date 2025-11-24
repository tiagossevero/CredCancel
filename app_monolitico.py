"""
CRED-CANCEL v3.0 - Sistema Integrado de Análise Fiscal
Receita Estadual de Santa Catarina - SEF/SC

ARQUIVO MONOLÍTICO - VERSÃO CONSOLIDADA
Desenvolvido por: AFRE Tiago Severo

Este arquivo consolida todas as funcionalidades do sistema em um único arquivo Python
para facilitar a implantação em servidores que requerem um único arquivo de execução.
"""

# =============================================================================
# IMPORTAÇÕES
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Union
import warnings
import ssl
from sqlalchemy import create_engine
import hashlib

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURAÇÕES DO SISTEMA
# =============================================================================

# Credenciais e Conexão
SENHA_ACESSO = "tsevero456"

# Configurações Impala
IMPALA_CONFIG = {
    'host': 'bdaworkernode02.sef.sc.gov.br',
    'port': 21050,
    'database': 'teste',
    'user': st.secrets.get("impala_credentials", {}).get("user", "tsevero"),
    'password': st.secrets.get("impala_credentials", {}).get("password", ""),
    'auth_mechanism': 'LDAP',
    'use_ssl': True
}

# Tabelas do Banco
TABELAS = {
    'completo': 'credito_dime_completo',
    'textil': 'credito_dime_textil',
    'metalmec': 'credito_dime_metalmec',
    'tech': 'credito_dime_tech'
}

# Períodos de Análise
PERIODOS = {
    '12m': {
        'label': '12 meses',
        'descricao': 'Out/2024 a Set/2025',
        'cor': '#1976d2',
        'icon': '📊'
    },
    '60m': {
        'label': '60 meses',
        'descricao': 'Set/2020 a Set/2025',
        'cor': '#2e7d32',
        'icon': '📈'
    },
    'comparativo': {
        'label': 'Comparativo',
        'descricao': '12m vs 60m',
        'cor': '#ef6c00',
        'icon': '🔄'
    }
}

# Contextos de Análise
CONTEXTOS = {
    'cancelamento': {
        'icon': '📋',
        'title': 'Análise para Cancelamento de IE',
        'description': 'Identificação de empresas candidatas ao cancelamento de Inscrição Estadual',
        'color': '#ef6c00'
    },
    'saldos_credores': {
        'icon': '💰',
        'title': 'Análise de Saldos Credores',
        'description': 'Verificação de créditos acumulados e padrões suspeitos',
        'color': '#2e7d32'
    },
    'ambos': {
        'icon': '🔄',
        'title': 'Análise Combinada',
        'description': 'Avaliação completa: Cancelamento + Saldos Credores',
        'color': '#1976d2'
    }
}

# Classificações de Risco
CLASSIFICACOES_RISCO = {
    'CRÍTICO': {'cor': '#c62828', 'peso': 4},
    'ALTO': {'cor': '#ef6c00', 'peso': 3},
    'MÉDIO': {'cor': '#fbc02d', 'peso': 2},
    'BAIXO': {'cor': '#388e3c', 'peso': 1}
}

# Níveis de Alerta ML
NIVEIS_ALERTA_ML = {
    'EMERGENCIAL': {'cor': '#3d0000', 'range': (85, 100), 'prioridade': 1},
    'CRÍTICO': {'cor': '#c62828', 'range': (70, 85), 'prioridade': 2},
    'ALTO': {'cor': '#ef6c00', 'range': (50, 70), 'prioridade': 3},
    'MÉDIO': {'cor': '#fbc02d', 'range': (30, 50), 'prioridade': 4},
    'BAIXO': {'cor': '#388e3c', 'range': (0, 30), 'prioridade': 5}
}

# Parâmetros de Machine Learning
ML_PARAMS = {
    'peso_score': 0.4,
    'peso_saldo': 0.3,
    'peso_estagnacao': 0.3,
    'bins_alerta': [0, 30, 50, 70, 85, 100],
    'labels_alerta': ['BAIXO', 'MÉDIO', 'ALTO', 'CRÍTICO', 'EMERGENCIAL']
}

# Thresholds e Limites
THRESHOLDS = {
    'saldo_alto': 500000,
    'saldo_medio': 50000,
    'crescimento_anormal': 200,
    'meses_estagnado_min': 6,
    'meses_estagnado_critico': 12,
    'indicios_fraude_critico': 5,
    'desvio_padrao_baixo': 1000,
    'cv_baixo': 0.1
}

# Cache e Performance
CACHE_CONFIG = {
    'ttl_dados': 3600,
    'ttl_metricas': 1800,
    'max_entries': 100
}

# Temas Visuais
TEMAS_PLOTLY = {
    'Claro': 'plotly_white',
    'Escuro': 'plotly_dark',
    'Padrão': 'plotly'
}

# Mapeamento de Colunas por Período
COLUNAS_PERIODO = {
    'score_risco': 'score_risco_{periodo}',
    'classificacao_risco': 'classificacao_risco_{periodo}',
    'score_risco_combinado': 'score_risco_combinado_{periodo}',
    'classificacao_risco_combinado': 'classificacao_risco_combinado_{periodo}',
    'valor_igual_total_periodos': 'valor_igual_total_periodos_{periodo}',
    'qtde_ultimos_meses_iguais': 'qtde_ultimos_12m_iguais',
    'media_credito': 'media_credito_{periodo}',
    'min_credito': 'min_credito_{periodo}',
    'max_credito': 'max_credito_{periodo}',
    'desvio_padrao_credito': 'desvio_padrao_credito_{periodo}',
    'qtde_meses_declarados': 'qtde_meses_declarados_{periodo}',
    'crescimento_saldo_percentual': 'crescimento_saldo_percentual_{periodo}',
    'crescimento_saldo_absoluto': 'crescimento_saldo_absoluto_{periodo}',
    'vl_credito_presumido': 'vl_credito_presumido_{periodo}',
    'qtde_tipos_cp': 'qtde_tipos_cp_{periodo}',
    'saldo_atras': 'saldo_{periodo}_atras'
}

# Configurações de Página
PAGE_CONFIG = {
    'page_title': 'CRED-CANCEL v3.0 - Sistema Integrado de Análise Fiscal',
    'page_icon': '💰',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
}

# Estilos CSS
CSS_STYLES = """
<style>
    /* HEADER PRINCIPAL */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1565c0;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }

    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2c3e50;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #1565c0;
        padding-bottom: 0.5rem;
    }

    /* GRÁFICOS PLOTLY */
    div[data-testid="stPlotlyChart"] {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        background-color: #ffffff;
    }

    /* MÉTRICAS */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 2px solid #2c3e50;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    div[data-testid="stMetric"] > label {
        font-weight: 600;
        color: #2c3e50;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1f77b4;
    }

    /* ALERTAS */
    .alert-critico {
        background-color: #ffebee;
        border-left: 5px solid #c62828;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .alert-alto {
        background-color: #fff3e0;
        border-left: 5px solid #ef6c00;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    /* TABELAS */
    .stDataFrame {
        font-size: 0.9rem;
        border-radius: 10px;
    }

    /* BOTÕES */
    .stButton>button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s;
    }

    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }

    /* EXPANDERS */
    .streamlit-expanderHeader {
        background-color: #f0f2f6;
        border-radius: 5px;
        font-weight: 600;
    }
</style>
"""

# Mapa de cores de risco
COLOR_MAP_RISCO = {
    'CRÍTICO': CLASSIFICACOES_RISCO['CRÍTICO']['cor'],
    'ALTO': CLASSIFICACOES_RISCO['ALTO']['cor'],
    'MÉDIO': CLASSIFICACOES_RISCO['MÉDIO']['cor'],
    'BAIXO': CLASSIFICACOES_RISCO['BAIXO']['cor']
}

# =============================================================================
# FUNÇÕES UTILITÁRIAS
# =============================================================================

def get_col_name(base_name: str, periodo: str = '12m') -> str:
    """Retorna o nome correto da coluna baseado no período."""
    if base_name in COLUNAS_PERIODO:
        col_template = COLUNAS_PERIODO[base_name]
        if '{periodo}' in col_template:
            return col_template.format(periodo=periodo)
        return col_template
    return base_name


def formatar_valor(valor: Union[int, float], tipo: str = 'numero') -> str:
    """Formata valores para exibição."""
    if pd.isna(valor):
        return 'N/A'

    try:
        if tipo == 'moeda':
            if abs(valor) >= 1e9:
                return f'R$ {valor/1e9:.2f}B'
            elif abs(valor) >= 1e6:
                return f'R$ {valor/1e6:.2f}M'
            elif abs(valor) >= 1e3:
                return f'R$ {valor/1e3:.1f}K'
            else:
                return f'R$ {valor:,.2f}'
        elif tipo == 'percentual':
            return f'{valor:.2f}%'
        elif tipo == 'decimal':
            return f'{valor:,.2f}'
        else:
            return f'{int(valor):,}'
    except:
        return str(valor)


def formatar_cnpj(cnpj: Union[str, int]) -> str:
    """Formata CNPJ para exibição."""
    cnpj_str = str(cnpj).zfill(14)
    if len(cnpj_str) != 14:
        return cnpj_str
    return f'{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:14]}'


def safe_divide(numerador: float, denominador: float, default: float = 0.0) -> float:
    """Divisão segura (evita divisão por zero)."""
    try:
        if denominador == 0:
            return default
        return numerador / denominador
    except:
        return default


def validar_cnpj(cnpj: str) -> bool:
    """Valida formato de CNPJ."""
    cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
    return len(cnpj_limpo) == 14


def calcular_variacao_percentual(valor_atual: float, valor_anterior: float) -> float:
    """Calcula variação percentual entre dois valores."""
    if valor_anterior == 0:
        return 0.0 if valor_atual == 0 else float('inf')
    return ((valor_atual - valor_anterior) / abs(valor_anterior)) * 100

# =============================================================================
# AUTENTICAÇÃO
# =============================================================================

def check_password():
    """Verifica autenticação do usuário."""
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

# =============================================================================
# CONEXÃO COM BANCO DE DADOS
# =============================================================================

# Configuração SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


@st.cache_resource
def get_impala_engine():
    """Cria e retorna engine de conexão com Impala."""
    try:
        connection_string = (
            f"impala://{IMPALA_CONFIG['host']}:"
            f"{IMPALA_CONFIG['port']}/"
            f"{IMPALA_CONFIG['database']}"
        )

        engine = create_engine(
            connection_string,
            connect_args={
                'user': IMPALA_CONFIG['user'],
                'password': IMPALA_CONFIG['password'],
                'auth_mechanism': IMPALA_CONFIG['auth_mechanism'],
                'use_ssl': IMPALA_CONFIG['use_ssl']
            }
        )

        with engine.connect() as conn:
            pass

        return engine

    except Exception as e:
        st.sidebar.error(f"❌ Erro na conexão: {str(e)[:150]}")
        return None


@st.cache_data(ttl=CACHE_CONFIG['ttl_dados'])
def carregar_dados_creditos(_engine) -> Dict[str, pd.DataFrame]:
    """Carrega dados de todas as tabelas DIME."""
    dados = {}

    if _engine is None:
        st.sidebar.error("❌ Engine de conexão inválida")
        return {}

    try:
        with _engine.connect() as conn:
            st.sidebar.success("✅ Conexão Impala OK!")
    except Exception as e:
        st.sidebar.error(f"❌ Falha na conexão: {str(e)[:100]}")
        return {}

    st.sidebar.write("📊 Carregando dados:")

    for key, table_name in TABELAS.items():
        try:
            st.sidebar.write(f"📊 Carregando {table_name}...")

            query = f"SELECT * FROM {IMPALA_CONFIG['database']}.{table_name}"
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]

            for col in df.select_dtypes(include=['object']).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass

            dados[key] = df

            # Debug detalhado
            if key == 'tech':
                st.sidebar.write(f"  ✓ Total registros: {len(df):,}")
                if 'flag_setor_tech' in df.columns:
                    flag_1 = (df['flag_setor_tech'] == 1).sum()
                    st.sidebar.write(f"  ✓ Com flag=1: {flag_1:,}")
                else:
                    st.sidebar.write(f"  ⚠️ flag_setor_tech NÃO EXISTE!")
            else:
                st.sidebar.success(f"  ✓ {table_name}: {len(df):,} linhas")

        except Exception as e:
            st.sidebar.error(f"❌ Erro em {table_name}")
            st.sidebar.caption(f"{str(e)[:80]}")
            dados[key] = pd.DataFrame()

    # Verificação final
    st.sidebar.write("---")
    st.sidebar.write("📋 Resumo Final:")
    for key in ['completo', 'textil', 'metalmec', 'tech']:
        df = dados.get(key, pd.DataFrame())
        if not df.empty:
            st.sidebar.write(f"  ✓ {key}: {len(df):,} registros")
        else:
            st.sidebar.write(f"  ❌ {key}: VAZIO")

    return dados

# =============================================================================
# FILTROS
# =============================================================================

def criar_filtros_sidebar(dados: Dict) -> Dict:
    """Cria painel de filtros na sidebar."""
    filtros = {}

    # Contexto da Análise
    with st.sidebar.expander("🎯 Contexto da Análise", expanded=True):
        st.markdown("**Selecione o objetivo da análise:**")

        contexto_opcoes = [
            f"{CONTEXTOS['cancelamento']['icon']} Cancelamento de IE",
            f"{CONTEXTOS['saldos_credores']['icon']} Verificação de Saldos Credores",
            f"{CONTEXTOS['ambos']['icon']} Ambos (Cancelamento + Saldos)"
        ]

        contexto_selecionado = st.radio(
            "Tipo de Análise:",
            contexto_opcoes,
            index=0,
            help="Define o foco da análise e adapta os indicadores e filtros"
        )

        if "Cancelamento" in contexto_selecionado and "Ambos" not in contexto_selecionado:
            filtros['contexto'] = 'cancelamento'
            st.info(f"🎯 {CONTEXTOS['cancelamento']['description']}")
        elif "Saldos" in contexto_selecionado and "Ambos" not in contexto_selecionado:
            filtros['contexto'] = 'saldos_credores'
            st.success(f"💰 {CONTEXTOS['saldos_credores']['description']}")
        else:
            filtros['contexto'] = 'ambos'
            st.warning(f"🔄 {CONTEXTOS['ambos']['description']}")

    # Período de Análise
    with st.sidebar.expander("📅 Período de Análise", expanded=True):
        st.markdown("**Selecione o período base:**")

        periodo_opcoes = [
            f"{PERIODOS['12m']['icon']} 12 meses ({PERIODOS['12m']['descricao']})",
            f"{PERIODOS['60m']['icon']} 60 meses ({PERIODOS['60m']['descricao']})",
            f"{PERIODOS['comparativo']['icon']} Comparativo (12m vs 60m)"
        ]

        periodo_selecionado = st.radio(
            "Análise baseada em:",
            periodo_opcoes,
            index=0,
            help="Define qual período será usado para scores e indicadores"
        )

        if "12 meses" in periodo_selecionado:
            filtros['periodo'] = '12m'
            st.info(f"📊 **Período:** {PERIODOS['12m']['descricao']}")
        elif "60 meses" in periodo_selecionado:
            filtros['periodo'] = '60m'
            st.success(f"📈 **Período:** {PERIODOS['60m']['descricao']}")
        else:
            filtros['periodo'] = 'comparativo'
            st.warning(f"🔄 **Modo:** {PERIODOS['comparativo']['descricao']}")

    # Filtros Globais
    with st.sidebar.expander("🔍 Filtros Globais", expanded=True):
        filtros['classificacoes'] = st.multiselect(
            "Classificações de Risco:",
            ['CRÍTICO', 'ALTO', 'MÉDIO', 'BAIXO'],
            default=['CRÍTICO', 'ALTO', 'MÉDIO', 'BAIXO'],
            help="Filtrar por nível de risco"
        )

        filtros['saldo_minimo'] = st.number_input(
            "Saldo Credor Mínimo (R$):",
            min_value=0,
            max_value=10000000,
            value=0,
            step=10000,
            format="%d",
            help="Saldo credor mínimo para exibir"
        )

        filtros['meses_iguais_min'] = st.slider(
            "Meses Estagnados (mínimo):",
            min_value=0,
            max_value=60,
            value=0,
            help="Número mínimo de meses com saldo igual"
        )

        df_completo = dados.get('completo', pd.DataFrame())
        if not df_completo.empty and 'nm_gerfe' in df_completo.columns:
            gerfes_disponiveis = sorted(df_completo['nm_gerfe'].dropna().unique().tolist())
            gerfes = ['TODAS'] + gerfes_disponiveis
            filtros['gerfe'] = st.selectbox(
                "GERFE (Gerência Regional):",
                gerfes,
                help="Filtrar por gerência regional"
            )

    # Filtros de Fraude
    with st.sidebar.expander("⚠️ Filtros de Fraude", expanded=False):
        filtros['apenas_suspeitas'] = st.checkbox(
            "Apenas empresas suspeitas",
            value=False,
            help="Filtrar apenas empresas marcadas como suspeitas"
        )

        filtros['apenas_canceladas'] = st.checkbox(
            "Apenas canceladas/inexistentes",
            value=False,
            help="Filtrar apenas empresas canceladas ou inexistentes"
        )

        filtros['min_indicios'] = st.slider(
            "Indícios de fraude (mínimo):",
            min_value=0,
            max_value=15,
            value=0,
            help="Número mínimo de indícios de fraude"
        )

        filtros['apenas_zeradas'] = st.checkbox(
            "Com declarações zeradas",
            value=False,
            help="Empresas com declarações zeradas"
        )

        filtros['apenas_omissas'] = st.checkbox(
            "Com omissões",
            value=False,
            help="Empresas com omissões de declarações"
        )

        filtros['apenas_noteiras'] = st.checkbox(
            "Empresas noteiras",
            value=False,
            help="Empresas identificadas como noteiras (emissoras de notas frias)"
        )

    # Filtros Avançados
    with st.sidebar.expander("⚙️ Filtros Avançados", expanded=False):
        filtros['score_min'] = st.slider(
            "Score de Risco Mínimo:",
            min_value=0,
            max_value=100,
            value=0,
            help="Score mínimo de risco"
        )

        filtros['crescimento_min'] = st.number_input(
            "Crescimento Mínimo (%):",
            min_value=-1000,
            max_value=10000,
            value=-1000,
            step=50,
            help="Crescimento percentual mínimo do saldo"
        )

    # Visualização
    with st.sidebar.expander("🎨 Visualização", expanded=False):
        tema_selecionado = st.selectbox(
            "Tema dos Gráficos:",
            list(TEMAS_PLOTLY.keys()),
            index=0,
            help="Tema visual dos gráficos"
        )
        filtros['tema'] = TEMAS_PLOTLY[tema_selecionado]

        filtros['mostrar_valores'] = st.checkbox(
            "Mostrar valores nos gráficos",
            value=True,
            help="Exibir valores numéricos nos gráficos"
        )

        filtros['top_n'] = st.slider(
            "Top N empresas em rankings:",
            min_value=5,
            max_value=100,
            value=20,
            step=5,
            help="Número de empresas a exibir em rankings"
        )

    return filtros


def aplicar_filtros(df: pd.DataFrame, filtros: Dict) -> pd.DataFrame:
    """Aplica filtros no DataFrame."""
    if df.empty:
        return df

    df_filtrado = df.copy()
    periodo = filtros.get('periodo', '12m')
    col_class = get_col_name('classificacao_risco', periodo)
    col_score = get_col_name('score_risco', periodo)
    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)

    # Filtros básicos
    if filtros.get('classificacoes') and len(filtros['classificacoes']) < 4:
        if col_class in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado[col_class].isin(filtros['classificacoes'])]

    if filtros.get('saldo_minimo', 0) > 0:
        df_filtrado = df_filtrado[df_filtrado['saldo_credor_atual'] >= filtros['saldo_minimo']]

    if filtros.get('meses_iguais_min', 0) > 0:
        df_filtrado = df_filtrado[df_filtrado['qtde_ultimos_12m_iguais'] >= filtros['meses_iguais_min']]

    if filtros.get('gerfe') and filtros['gerfe'] != 'TODAS':
        if 'nm_gerfe' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['nm_gerfe'] == filtros['gerfe']]

    # Filtros de fraude
    if filtros.get('apenas_suspeitas') and 'flag_empresa_suspeita' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['flag_empresa_suspeita'] == 1]

    if filtros.get('apenas_canceladas') and 'sn_cancelado_inex_inativ' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['sn_cancelado_inex_inativ'] == 1]

    if filtros.get('min_indicios', 0) > 0 and 'qtde_indicios_fraude' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['qtde_indicios_fraude'] >= filtros['min_indicios']]

    if filtros.get('apenas_zeradas') and 'flag_tem_declaracoes_zeradas' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['flag_tem_declaracoes_zeradas'] == 1]

    if filtros.get('apenas_omissas') and 'flag_tem_omissoes' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['flag_tem_omissoes'] == 1]

    if filtros.get('apenas_noteiras') and 'flag_empresa_noteira' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['flag_empresa_noteira'] == 1]

    # Filtros avançados
    if filtros.get('score_min', 0) > 0 and col_score in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado[col_score] >= filtros['score_min']]

    if filtros.get('crescimento_min', -1000) > -1000 and col_cresc in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado[col_cresc] >= filtros['crescimento_min']]

    return df_filtrado


def get_resumo_filtros(filtros: Dict, df_original: pd.DataFrame,
                      df_filtrado: pd.DataFrame) -> Dict:
    """Gera resumo dos filtros aplicados."""
    total_original = len(df_original)
    total_filtrado = len(df_filtrado)
    pct_mantido = (total_filtrado / total_original * 100) if total_original > 0 else 0

    return {
        'total_original': total_original,
        'total_filtrado': total_filtrado,
        'removidos': total_original - total_filtrado,
        'pct_mantido': pct_mantido,
        'contexto': filtros.get('contexto', 'ambos'),
        'periodo': filtros.get('periodo', '12m')
    }

# =============================================================================
# MÉTRICAS E CÁLCULOS
# =============================================================================

def calcular_kpis_gerais(df: pd.DataFrame, periodo: str = '12m') -> Dict:
    """Calcula KPIs principais do sistema."""
    if df.empty:
        return {k: 0 for k in [
            'total_empresas', 'total_grupos', 'saldo_total', 'score_medio',
            'score_combinado_medio', 'criticos', 'altos', 'medios', 'baixos',
            'congelados_12m', 'crescimento_medio', 'cp_total',
            'empresas_suspeitas', 'empresas_canceladas',
            'empresas_5plus_indicios', 'saldo_medio_empresa',
            'saldo_mediano', 'desvio_padrao_saldo'
        ]}

    col_score = get_col_name('score_risco', periodo)
    col_classificacao = get_col_name('classificacao_risco', periodo)
    col_crescimento = get_col_name('crescimento_saldo_percentual', periodo)
    col_cp = get_col_name('vl_credito_presumido', periodo)
    col_score_comb = get_col_name('score_risco_combinado', periodo)

    kpis = {
        'total_empresas': df['nu_cnpj'].nunique(),
        'total_grupos': df['nu_cnpj_grupo'].nunique() if 'nu_cnpj_grupo' in df.columns else 0,
        'saldo_total': float(df['saldo_credor_atual'].sum()),
        'saldo_medio_empresa': float(df['saldo_credor_atual'].mean()),
        'saldo_mediano': float(df['saldo_credor_atual'].median()),
        'desvio_padrao_saldo': float(df['saldo_credor_atual'].std()),
        'score_medio': float(df[col_score].mean()) if col_score in df.columns else 0,
        'score_combinado_medio': float(df[col_score_comb].mean()) if col_score_comb in df.columns else 0,
        'criticos': len(df[df[col_classificacao] == 'CRÍTICO']) if col_classificacao in df.columns else 0,
        'altos': len(df[df[col_classificacao] == 'ALTO']) if col_classificacao in df.columns else 0,
        'medios': len(df[df[col_classificacao] == 'MÉDIO']) if col_classificacao in df.columns else 0,
        'baixos': len(df[df[col_classificacao] == 'BAIXO']) if col_classificacao in df.columns else 0,
        'congelados_12m': len(df[df['qtde_ultimos_12m_iguais'] >= 12]),
        'congelados_6m': len(df[df['qtde_ultimos_12m_iguais'] >= 6]),
        'crescimento_medio': float(df[col_crescimento].mean()) if col_crescimento in df.columns else 0,
        'cp_total': float(df[col_cp].sum()) if col_cp in df.columns else 0,
        'empresas_suspeitas': len(df[df['flag_empresa_suspeita'] == 1]) if 'flag_empresa_suspeita' in df.columns else 0,
        'empresas_canceladas': len(df[df['sn_cancelado_inex_inativ'] == 1]) if 'sn_cancelado_inex_inativ' in df.columns else 0,
        'empresas_5plus_indicios': len(df[df['qtde_indicios_fraude'] >= 5]) if 'qtde_indicios_fraude' in df.columns else 0,
    }

    if kpis['total_empresas'] > 0:
        kpis['pct_criticos'] = (kpis['criticos'] / kpis['total_empresas']) * 100
        kpis['pct_altos'] = (kpis['altos'] / kpis['total_empresas']) * 100
        kpis['pct_suspeitas'] = (kpis['empresas_suspeitas'] / kpis['total_empresas']) * 100
        kpis['pct_congelados'] = (kpis['congelados_12m'] / kpis['total_empresas']) * 100

    return kpis


def calcular_estatisticas_setoriais(dados: Dict, periodo: str = '12m') -> pd.DataFrame:
    """Calcula estatísticas dos setores."""
    setores = []
    col_score = get_col_name('score_risco', periodo)
    col_class = get_col_name('classificacao_risco', periodo)

    # Debug setorial
    st.sidebar.write("---")
    st.sidebar.write("🔍 DEBUG Setorial:")
    st.sidebar.write(f"Período: {periodo}")
    st.sidebar.write(f"Dados recebidos: {list(dados.keys())}")

    for setor_key, setor_nome, flag_col in [
        ('textil', 'TÊXTIL', 'flag_setor_textil'),
        ('metalmec', 'METAL-MECÂNICO', 'flag_setor_metalmec'),
        ('tech', 'TECNOLOGIA', 'flag_setor_tech')
    ]:
        df = dados.get(setor_key, pd.DataFrame())

        st.sidebar.write(f"  {setor_key}:")
        st.sidebar.write(f"    Vazio? {df.empty}")

        if df.empty:
            st.sidebar.write(f"    ⚠️ DataFrame vazio, pulando...")
            continue

        st.sidebar.write(f"    Total registros: {len(df)}")

        # Verificar se a coluna flag existe
        if flag_col in df.columns:
            df_ativo = df[df[flag_col] == 1]
            st.sidebar.write(f"    Com flag=1: {len(df_ativo)}")
        else:
            st.sidebar.write(f"    ⚠️ Coluna {flag_col} não existe, usando todos")
            df_ativo = df

        # Se ainda está vazio após o filtro, pular
        if df_ativo.empty:
            st.sidebar.write(f"    ❌ Vazio após filtro!")
            continue

        setor_info = {
            'Setor': setor_nome,
            'Empresas': df_ativo['nu_cnpj'].nunique(),
            'Saldo Total': float(df_ativo['saldo_credor_atual'].sum()),
            'Saldo Médio': float(df_ativo['saldo_credor_atual'].mean()),
            'Score Médio': float(df_ativo[col_score].mean()) if col_score in df_ativo.columns else 0,
            'Críticos': len(df_ativo[df_ativo[col_class] == 'CRÍTICO']) if col_class in df_ativo.columns else 0,
            'Altos': len(df_ativo[df_ativo[col_class] == 'ALTO']) if col_class in df_ativo.columns else 0,
            'Congelados 12m+': len(df_ativo[df_ativo['qtde_ultimos_12m_iguais'] >= 12])
        }

        if 'flag_empresa_suspeita' in df_ativo.columns:
            setor_info['Suspeitas'] = len(df_ativo[df_ativo['flag_empresa_suspeita'] == 1])

        st.sidebar.write(f"    ✓ Processado com sucesso")
        setores.append(setor_info)

    st.sidebar.write(f"Total setores processados: {len(setores)}")

    return pd.DataFrame(setores)


def calcular_score_ml(df: pd.DataFrame, periodo: str = '12m',
                     peso_score: float = None, peso_saldo: float = None,
                     peso_estagnacao: float = None) -> pd.DataFrame:
    """Calcula score de Machine Learning para priorização."""
    df_ml = df.copy()

    if peso_score is None:
        peso_score = ML_PARAMS['peso_score']
    if peso_saldo is None:
        peso_saldo = ML_PARAMS['peso_saldo']
    if peso_estagnacao is None:
        peso_estagnacao = ML_PARAMS['peso_estagnacao']

    col_score = get_col_name('score_risco', periodo)

    max_score = df_ml[col_score].max() if col_score in df_ml.columns and df_ml[col_score].max() > 0 else 1
    max_saldo = df_ml['saldo_credor_atual'].max() if df_ml['saldo_credor_atual'].max() > 0 else 1

    df_ml['score_norm'] = (df_ml[col_score] / max_score * 100) if col_score in df_ml.columns else 0
    df_ml['saldo_norm'] = (df_ml['saldo_credor_atual'] / max_saldo * 100)
    df_ml['estagnacao_norm'] = (df_ml['qtde_ultimos_12m_iguais'] / 13 * 100)

    df_ml['score_ml'] = (
        df_ml['score_norm'] * peso_score +
        df_ml['saldo_norm'] * peso_saldo +
        df_ml['estagnacao_norm'] * peso_estagnacao
    )

    df_ml['nivel_alerta_ml'] = pd.cut(
        df_ml['score_ml'],
        bins=ML_PARAMS['bins_alerta'],
        labels=ML_PARAMS['labels_alerta']
    )

    df_ml['prioridade_ml'] = df_ml['score_ml'].rank(method='dense', ascending=False)

    return df_ml


def calcular_indicadores_fraude(df: pd.DataFrame, periodo: str = '12m') -> pd.DataFrame:
    """Calcula indicadores avançados de fraude."""
    df_fraude = df.copy()

    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)
    col_desvio = get_col_name('desvio_padrao_credito', periodo)
    col_media = get_col_name('media_credito', periodo)

    if col_cresc in df_fraude.columns:
        df_fraude['ind_crescimento_anormal'] = (
            df_fraude[col_cresc] > THRESHOLDS['crescimento_anormal']
        ).astype(int)

    df_fraude['ind_alto_estagnado'] = (
        (df_fraude['saldo_credor_atual'] > THRESHOLDS['saldo_medio']) &
        (df_fraude['qtde_ultimos_12m_iguais'] >= THRESHOLDS['meses_estagnado_min'])
    ).astype(int)

    if col_desvio in df_fraude.columns and col_media in df_fraude.columns:
        df_fraude['ind_baixa_variacao'] = (
            (df_fraude[col_desvio] < THRESHOLDS['desvio_padrao_baixo']) &
            (df_fraude[col_media] > THRESHOLDS['saldo_medio'])
        ).astype(int)

    df_fraude['ind_saldo_extremo'] = (
        df_fraude['saldo_credor_atual'] > THRESHOLDS['saldo_alto']
    ).astype(int)

    cols_indicadores = [col for col in df_fraude.columns if col.startswith('ind_')]
    if cols_indicadores:
        df_fraude['score_fraude_calculado'] = df_fraude[cols_indicadores].sum(axis=1)

    return df_fraude


def calcular_metricas_comparativas(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula métricas comparativas entre 12m e 60m."""
    df_comp = df.copy()

    if 'score_risco_12m' in df_comp.columns and 'score_risco_60m' in df_comp.columns:
        df_comp['variacao_score'] = df_comp['score_risco_12m'] - df_comp['score_risco_60m']
        df_comp['variacao_score_pct'] = safe_divide(
            df_comp['variacao_score'],
            df_comp['score_risco_60m'],
            0
        ) * 100

    if 'classificacao_risco_12m' in df_comp.columns and 'classificacao_risco_60m' in df_comp.columns:
        def classificar_mudanca(row):
            class_12m = row.get('classificacao_risco_12m', 'BAIXO')
            class_60m = row.get('classificacao_risco_60m', 'BAIXO')
            peso_map = {'CRÍTICO': 4, 'ALTO': 3, 'MÉDIO': 2, 'BAIXO': 1}
            peso_12m = peso_map.get(class_12m, 1)
            peso_60m = peso_map.get(class_60m, 1)
            if peso_12m > peso_60m:
                return 'PIORA'
            elif peso_12m < peso_60m:
                return 'MELHORA'
            else:
                return 'ESTÁVEL'

        df_comp['mudanca_classificacao'] = df_comp.apply(classificar_mudanca, axis=1)

    return df_comp


def calcular_concentracao_risco(df: pd.DataFrame, group_col: str = 'nm_gerfe') -> Dict:
    """Calcula concentração de risco por agrupamento."""
    if df.empty or group_col not in df.columns:
        return {}

    saldo_grupo = df.groupby(group_col)['saldo_credor_atual'].sum().sort_values(ascending=False)
    top5_saldo = saldo_grupo.head(5).sum()
    top10_saldo = saldo_grupo.head(10).sum()
    total_saldo = saldo_grupo.sum()

    n = len(saldo_grupo)
    if n > 0 and total_saldo > 0:
        sorted_valores = np.sort(saldo_grupo.values)
        cumsum = np.cumsum(sorted_valores)
        gini = (2 * np.sum((np.arange(1, n + 1)) * sorted_valores)) / (n * total_saldo) - (n + 1) / n
    else:
        gini = 0

    return {
        'total_grupos': len(saldo_grupo),
        'saldo_total': total_saldo,
        'top5_saldo': top5_saldo,
        'top10_saldo': top10_saldo,
        'pct_top5': (top5_saldo / total_saldo * 100) if total_saldo > 0 else 0,
        'pct_top10': (top10_saldo / total_saldo * 100) if total_saldo > 0 else 0,
        'gini': gini,
        'concentracao': 'ALTA' if gini > 0.6 else 'MÉDIA' if gini > 0.4 else 'BAIXA'
    }

# =============================================================================
# VISUALIZAÇÕES
# =============================================================================

def criar_grafico_pizza(df: pd.DataFrame, coluna_valores: str, coluna_nomes: str,
                        titulo: str, tema: str = 'plotly_white',
                        color_map: Optional[Dict] = None) -> go.Figure:
    """Cria gráfico de pizza."""
    if df.empty:
        return go.Figure()

    fig = px.pie(
        df,
        values=coluna_valores,
        names=coluna_nomes,
        title=titulo,
        template=tema,
        color=coluna_nomes,
        color_discrete_map=color_map,
        hole=0.4
    )

    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def criar_grafico_barras(df: pd.DataFrame, x: str, y: str, titulo: str,
                         tema: str = 'plotly_white', color: Optional[str] = None,
                         color_map: Optional[Dict] = None,
                         orientacao: str = 'v') -> go.Figure:
    """Cria gráfico de barras."""
    if df.empty:
        return go.Figure()

    fig = px.bar(
        df,
        x=x if orientacao == 'v' else y,
        y=y if orientacao == 'v' else x,
        title=titulo,
        template=tema,
        color=color,
        color_discrete_map=color_map,
        orientation=orientacao
    )

    fig.update_layout(showlegend=True if color else False)
    return fig


def criar_grafico_dispersao(df: pd.DataFrame, x: str, y: str, titulo: str,
                            tema: str = 'plotly_white', color: Optional[str] = None,
                            size: Optional[str] = None, hover_data: Optional[List] = None) -> go.Figure:
    """Cria gráfico de dispersão."""
    if df.empty:
        return go.Figure()

    fig = px.scatter(
        df,
        x=x,
        y=y,
        title=titulo,
        template=tema,
        color=color,
        size=size,
        hover_data=hover_data
    )

    return fig


def criar_ranking_horizontal(df: pd.DataFrame, label_col: str, value_col: str,
                             titulo: str, top_n: int = 10,
                             tema: str = 'plotly_white') -> go.Figure:
    """Cria ranking horizontal."""
    if df.empty:
        return go.Figure()

    df_top = df.nlargest(top_n, value_col)

    fig = px.bar(
        df_top,
        x=value_col,
        y=label_col,
        orientation='h',
        title=titulo,
        template=tema,
        color=value_col,
        color_continuous_scale='Reds'
    )

    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    return fig


def criar_grafico_comparativo_dual(df: pd.DataFrame, x: str, y1: str, y2: str,
                                   titulo: str, label_y1: str, label_y2: str,
                                   tema: str = 'plotly_white') -> go.Figure:
    """Cria gráfico com dois eixos Y."""
    if df.empty:
        return go.Figure()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(x=df[x], y=df[y1], name=label_y1),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=df[x], y=df[y2], name=label_y2, mode='lines+markers'),
        secondary_y=True,
    )

    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=label_y1, secondary_y=False)
    fig.update_yaxes(title_text=label_y2, secondary_y=True)

    fig.update_layout(
        title_text=titulo,
        template=tema
    )

    return fig


def criar_heatmap(df: pd.DataFrame, titulo: str,
                 tema: str = 'plotly_white') -> go.Figure:
    """Cria heatmap de correlação."""
    if df.empty:
        return go.Figure()

    df_num = df.select_dtypes(include=['number'])

    if df_num.empty:
        return go.Figure()

    corr = df_num.corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr.values,
        x=corr.columns,
        y=corr.columns,
        colorscale='RdBu',
        zmid=0
    ))

    fig.update_layout(
        title=titulo,
        template=tema
    )

    return fig


def criar_gauge(valor: float, titulo: str, max_val: float = 100,
               tema: str = 'plotly_white') -> go.Figure:
    """Cria gauge (velocímetro)."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=valor,
        title={'text': titulo},
        gauge={
            'axis': {'range': [None, max_val]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, max_val * 0.3], 'color': "lightgreen"},
                {'range': [max_val * 0.3, max_val * 0.7], 'color': "yellow"},
                {'range': [max_val * 0.7, max_val], 'color': "red"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_val * 0.9
            }
        }
    ))

    fig.update_layout(template=tema)
    return fig

# =============================================================================
# EXPORTAÇÃO DE DADOS
# =============================================================================

def exportar_para_excel(df: pd.DataFrame, nome_arquivo: Optional[str] = None) -> BytesIO:
    """Exporta DataFrame para Excel."""
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Dados', index=False)

        workbook = writer.book
        worksheet = writer.sheets['Dados']

        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#1565c0',
            'font_color': 'white',
            'border': 1
        })

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, col_num, 15)

    output.seek(0)
    return output


def exportar_para_csv(df: pd.DataFrame) -> BytesIO:
    """Exporta DataFrame para CSV."""
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8-sig', sep=';')
    output.seek(0)
    return output


def criar_relatorio_completo(df: pd.DataFrame, kpis: Dict,
                             titulo: str = "Relatório CRED-CANCEL") -> BytesIO:
    """Cria relatório completo com dados e KPIs."""
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_resumo = pd.DataFrame([kpis]).T
        df_resumo.columns = ['Valor']
        df_resumo.to_excel(writer, sheet_name='Resumo')

        df.to_excel(writer, sheet_name='Dados Completos', index=False)

        workbook = writer.book
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'fg_color': '#1565c0',
            'font_color': 'white'
        })

        worksheet_resumo = writer.sheets['Resumo']
        worksheet_resumo.write(0, 0, titulo, title_format)

    output.seek(0)
    return output


def criar_botao_download_excel(df: pd.DataFrame, label: str = "📥 Baixar Excel",
                               nome_arquivo: Optional[str] = None):
    """Cria botão de download para Excel."""
    if nome_arquivo is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"cred_cancel_{timestamp}.xlsx"

    excel_data = exportar_para_excel(df, nome_arquivo)

    st.download_button(
        label=label,
        data=excel_data,
        file_name=nome_arquivo,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def criar_botao_download_csv(df: pd.DataFrame, label: str = "📥 Baixar CSV",
                             nome_arquivo: Optional[str] = None):
    """Cria botão de download para CSV."""
    if nome_arquivo is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"cred_cancel_{timestamp}.csv"

    csv_data = exportar_para_csv(df)

    st.download_button(
        label=label,
        data=csv_data,
        file_name=nome_arquivo,
        mime="text/csv"
    )


def criar_painel_exportacao(df: pd.DataFrame, kpis: Optional[Dict] = None,
                           dados_extras: Optional[Dict[str, pd.DataFrame]] = None):
    """Cria painel completo de exportação."""
    st.subheader("📥 Exportar Dados")

    col1, col2, col3 = st.columns(3)

    with col1:
        criar_botao_download_excel(df, "📊 Excel - Dados")

    with col2:
        criar_botao_download_csv(df, "📄 CSV - Dados")

    with col3:
        if kpis:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_relatorio = f"relatorio_completo_{timestamp}.xlsx"
            relatorio = criar_relatorio_completo(df, kpis)

            st.download_button(
                label="📑 Relatório Completo",
                data=relatorio,
                file_name=nome_relatorio,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# =============================================================================
# APLICAÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal da aplicação."""

    # Configuração da página
    st.set_page_config(**PAGE_CONFIG)

    # Aplicar estilos CSS
    st.markdown(CSS_STYLES, unsafe_allow_html=True)

    # Autenticação
    check_password()

    # Header principal
    st.markdown(
        "<h1 style='text-align: center; color: #1565c0; margin-bottom: 0;'>"
        "💰 CRED-CANCEL v3.0</h1>"
        "<p style='text-align: center; color: #666; margin-top: 0;'>"
        "Sistema Integrado de Análise Fiscal - SEF/SC</p>",
        unsafe_allow_html=True
    )

    # Carregar dados
    engine = get_impala_engine()

    if engine is None:
        st.error("❌ Não foi possível conectar ao banco de dados.")
        st.stop()

    with st.spinner("⏳ Carregando dados do Impala..."):
        dados = carregar_dados_creditos(engine)

    if not dados or all(df.empty for df in dados.values()):
        st.error("❌ Nenhum dado foi carregado. Verifique a conexão.")
        st.stop()

    df_completo = dados.get('completo', pd.DataFrame())

    if df_completo.empty:
        st.error("❌ DataFrame principal vazio.")
        st.stop()

    # Sidebar - Filtros e Navegação
    st.sidebar.title("⚙️ Configurações")

    filtros = criar_filtros_sidebar(dados)
    df_filtrado = aplicar_filtros(df_completo, filtros)

    st.sidebar.write("---")
    st.sidebar.write("📊 **Resumo:**")
    resumo = get_resumo_filtros(filtros, df_completo, df_filtrado)
    st.sidebar.metric("Empresas Filtradas", f"{resumo['total_filtrado']:,}")
    st.sidebar.caption(f"{resumo['pct_mantido']:.1f}% do total")

    # Botão de logout
    st.sidebar.write("---")
    if st.sidebar.button("🚪 Sair", use_container_width=True):
        logout()

    # Menu de navegação
    st.sidebar.write("---")
    st.sidebar.title("📑 Navegação")

    menu_opcoes = [
        "🏠 Dashboard Executivo",
        "📊 Análise Comparativa 12m vs 60m",
        "🔍 Análise de Suspeitas",
        "🏆 Ranking de Empresas",
        "🏭 Análise Setorial",
        "🔬 Drill-Down de Empresa",
        "🤖 Machine Learning & IA",
        "⚠️ Padrões de Abuso",
        "💤 Empresas Inativas",
        "📋 Empresas Noteiras",
        "0️⃣ Declarações Zeradas",
        "ℹ️ Sobre o Sistema"
    ]

    pagina_selecionada = st.sidebar.radio("Escolha uma página:", menu_opcoes, label_visibility="collapsed")

    # Obter período e contexto
    periodo = filtros.get('periodo', '12m')
    contexto = filtros.get('contexto', 'ambos')
    tema = filtros.get('tema', 'plotly_white')

    # Banner do período
    if periodo in PERIODOS:
        st.markdown(
            f"<div style='background: {PERIODOS[periodo]['cor']}; color: white; "
            f"padding: 15px; border-radius: 10px; margin-bottom: 20px; text-align: center;'>"
            f"<b>{PERIODOS[periodo]['icon']} Período de Análise: {PERIODOS[periodo]['label'].upper()} "
            f"({PERIODOS[periodo]['descricao']})</b>"
            f"</div>",
            unsafe_allow_html=True
        )

    # =============================================================================
    # PÁGINAS DO DASHBOARD
    # =============================================================================

    if "Dashboard Executivo" in pagina_selecionada:
        contexto_info = CONTEXTOS.get(contexto, CONTEXTOS['ambos'])

        st.markdown(f"<h1 class='main-header'>{contexto_info['icon']} {contexto_info['title']}</h1>",
                    unsafe_allow_html=True)

        kpis = calcular_kpis_gerais(df_filtrado, periodo)

        st.subheader("📊 Indicadores Principais")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Empresas Monitoradas", formatar_valor(kpis['total_empresas'], 'numero'))

        with col2:
            st.metric("Grupos Econômicos", formatar_valor(kpis['total_grupos'], 'numero'))

        with col3:
            st.metric("Saldo Credor Total", formatar_valor(kpis['saldo_total'], 'moeda'))

        with col4:
            st.metric("Score Médio", f"{kpis['score_medio']:.1f}")

        with col5:
            st.metric("Casos Críticos", formatar_valor(kpis['criticos'], 'numero'),
                     delta=f"{kpis['altos']:,} altos", delta_color="inverse")

        st.subheader(f"{contexto_info['icon']} Indicadores Contextuais")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Congelados 12m+", formatar_valor(kpis['congelados_12m'], 'numero'))

        with col2:
            if kpis['empresas_suspeitas'] > 0:
                pct_susp = (kpis['empresas_suspeitas'] / kpis['total_empresas'] * 100)
                st.metric("Empresas Suspeitas", formatar_valor(kpis['empresas_suspeitas'], 'numero'),
                         delta=f"{pct_susp:.1f}%", delta_color="inverse")
            else:
                st.metric("Empresas Suspeitas", "0")

        with col3:
            st.metric("5+ Indícios Fraude", formatar_valor(kpis['empresas_5plus_indicios'], 'numero'),
                     delta_color="inverse")

        with col4:
            st.metric("Empresas Canceladas", formatar_valor(kpis['empresas_canceladas'], 'numero'),
                     delta_color="inverse")

        st.divider()

        st.subheader("📈 Análises Visuais")

        col1, col2 = st.columns(2)

        col_class = get_col_name('classificacao_risco', periodo)

        with col1:
            if col_class in df_filtrado.columns:
                dist_risco = df_filtrado[col_class].value_counts().reset_index()
                dist_risco.columns = ['Classificação', 'Quantidade']

                fig = criar_grafico_pizza(
                    dist_risco, 'Quantidade', 'Classificação',
                    f'Distribuição por Risco ({periodo.upper()})',
                    tema, COLOR_MAP_RISCO
                )
                st.plotly_chart(fig, use_container_width=True, key="exec_pizza_risco")

        with col2:
            if col_class in df_filtrado.columns:
                saldo_risco = df_filtrado.groupby(col_class)['saldo_credor_atual'].sum().reset_index()
                saldo_risco.columns = ['Classificação', 'Saldo']

                fig = criar_grafico_barras(
                    saldo_risco, 'Classificação', 'Saldo',
                    f'Saldo Credor por Risco ({periodo.upper()})',
                    tema, 'Classificação', COLOR_MAP_RISCO
                )
                st.plotly_chart(fig, use_container_width=True, key="exec_bar_saldo")

        st.subheader("🎯 Concentração de Risco por GERFE")

        if 'nm_gerfe' in df_filtrado.columns:
            concentracao = calcular_concentracao_risco(df_filtrado, 'nm_gerfe')

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total GERFEs", concentracao.get('total_grupos', 0))

            with col2:
                st.metric("Top 5 (% Saldo)", f"{concentracao.get('pct_top5', 0):.1f}%")

            with col3:
                st.metric("Top 10 (% Saldo)", f"{concentracao.get('pct_top10', 0):.1f}%")

            with col4:
                st.metric("Concentração", concentracao.get('concentracao', 'N/A'))

            saldo_gerfe = df_filtrado.groupby('nm_gerfe')['saldo_credor_atual'].agg(['sum', 'count']).reset_index()
            saldo_gerfe.columns = ['GERFE', 'Saldo Total', 'Quantidade']

            fig = criar_ranking_horizontal(
                saldo_gerfe, 'GERFE', 'Saldo Total',
                'Top 10 GERFEs por Saldo Credor', 10, tema
            )
            st.plotly_chart(fig, use_container_width=True, key="exec_ranking_gerfe")

        st.divider()
        criar_painel_exportacao(df_filtrado, kpis)

    elif "Comparativa" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>📊 Análise Comparativa: 12m vs 60m</h1>",
                    unsafe_allow_html=True)

        df_comp = calcular_metricas_comparativas(df_filtrado)
        kpis_12m = calcular_kpis_gerais(df_filtrado, '12m')
        kpis_60m = calcular_kpis_gerais(df_filtrado, '60m')

        st.subheader("🔄 Comparação de Indicadores")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### 📊 12 Meses")
            st.metric("Score Médio", f"{kpis_12m['score_medio']:.1f}")
            st.metric("Críticos", formatar_valor(kpis_12m['criticos'], 'numero'))
            st.metric("Saldo Total", formatar_valor(kpis_12m['saldo_total'], 'moeda'))

        with col2:
            st.markdown("### 📈 60 Meses")
            st.metric("Score Médio", f"{kpis_60m['score_medio']:.1f}")
            st.metric("Críticos", formatar_valor(kpis_60m['criticos'], 'numero'))
            st.metric("Saldo Total", formatar_valor(kpis_60m['saldo_total'], 'moeda'))

        with col3:
            st.markdown("### 🔄 Variação")
            delta_score = kpis_12m['score_medio'] - kpis_60m['score_medio']
            st.metric("Δ Score", f"{delta_score:+.1f}")
            delta_crit = kpis_12m['criticos'] - kpis_60m['criticos']
            st.metric("Δ Críticos", f"{delta_crit:+,}")
            delta_saldo = kpis_12m['saldo_total'] - kpis_60m['saldo_total']
            st.metric("Δ Saldo", formatar_valor(delta_saldo, 'moeda'))

        st.divider()

        st.subheader("📊 Visualizações Comparativas")

        if 'classificacao_risco_12m' in df_comp.columns and 'classificacao_risco_60m' in df_comp.columns:
            col1, col2 = st.columns(2)

            with col1:
                dist_12m = df_comp['classificacao_risco_12m'].value_counts().reset_index()
                dist_12m.columns = ['Classificação', 'Quantidade']

                fig = criar_grafico_barras(
                    dist_12m, 'Classificação', 'Quantidade',
                    'Distribuição de Risco - 12 Meses',
                    tema, 'Classificação', COLOR_MAP_RISCO
                )
                st.plotly_chart(fig, use_container_width=True, key="comp_12m")

            with col2:
                dist_60m = df_comp['classificacao_risco_60m'].value_counts().reset_index()
                dist_60m.columns = ['Classificação', 'Quantidade']

                fig = criar_grafico_barras(
                    dist_60m, 'Classificação', 'Quantidade',
                    'Distribuição de Risco - 60 Meses',
                    tema, 'Classificação', COLOR_MAP_RISCO
                )
                st.plotly_chart(fig, use_container_width=True, key="comp_60m")

        if 'mudanca_classificacao' in df_comp.columns:
            mudancas = df_comp['mudanca_classificacao'].value_counts().reset_index()
            mudancas.columns = ['Mudança', 'Quantidade']

            fig = criar_grafico_pizza(
                mudancas, 'Quantidade', 'Mudança',
                'Mudanças de Classificação (60m → 12m)',
                tema
            )
            st.plotly_chart(fig, use_container_width=True, key="comp_mudancas")

        criar_painel_exportacao(df_comp, {'kpis_12m': kpis_12m, 'kpis_60m': kpis_60m})

    elif "Suspeitas" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>🔍 Análise de Empresas Suspeitas</h1>",
                    unsafe_allow_html=True)

        if 'flag_empresa_suspeita' in df_filtrado.columns:
            df_suspeitas = df_filtrado[df_filtrado['flag_empresa_suspeita'] == 1]
        else:
            df_suspeitas = pd.DataFrame()

        if df_suspeitas.empty:
            st.warning("⚠️ Nenhuma empresa suspeita encontrada com os filtros atuais.")
        else:
            st.success(f"✅ Encontradas **{len(df_suspeitas):,}** empresas suspeitas")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                saldo_susp = df_suspeitas['saldo_credor_atual'].sum()
                st.metric("Saldo Total Suspeito", formatar_valor(saldo_susp, 'moeda'))

            with col2:
                if 'qtde_indicios_fraude' in df_suspeitas.columns:
                    ind_medio = df_suspeitas['qtde_indicios_fraude'].mean()
                    st.metric("Indícios Médios", f"{ind_medio:.1f}")

            with col3:
                congeladas = len(df_suspeitas[df_suspeitas['qtde_ultimos_12m_iguais'] >= 12])
                st.metric("Congeladas 12m+", formatar_valor(congeladas, 'numero'))

            with col4:
                col_score = get_col_name('score_risco', periodo)
                if col_score in df_suspeitas.columns:
                    score_med = df_suspeitas[col_score].mean()
                    st.metric("Score Médio", f"{score_med:.1f}")

            st.divider()

            st.subheader("🏆 Top 20 Empresas Mais Suspeitas")

            cols_exibir = ['nu_cnpj', 'nm_razao_social', 'saldo_credor_atual']

            if 'qtde_indicios_fraude' in df_suspeitas.columns:
                cols_exibir.append('qtde_indicios_fraude')

            col_score = get_col_name('score_risco', periodo)
            if col_score in df_suspeitas.columns:
                cols_exibir.append(col_score)
                df_top = df_suspeitas.nlargest(20, col_score)[cols_exibir]
            else:
                df_top = df_suspeitas.head(20)[cols_exibir]

            st.dataframe(df_top, use_container_width=True, hide_index=True)

            st.subheader("📊 Análises Visuais")

            if 'qtde_indicios_fraude' in df_suspeitas.columns:
                col1, col2 = st.columns(2)

                with col1:
                    hist_data = df_suspeitas['qtde_indicios_fraude'].value_counts().reset_index()
                    hist_data.columns = ['Indícios', 'Quantidade']
                    hist_data = hist_data.sort_values('Indícios')

                    fig = criar_grafico_barras(
                        hist_data, 'Indícios', 'Quantidade',
                        'Distribuição de Indícios de Fraude',
                        tema
                    )
                    st.plotly_chart(fig, use_container_width=True, key="susp_indicios")

                with col2:
                    saldo_ind = df_suspeitas.groupby('qtde_indicios_fraude')['saldo_credor_atual'].sum().reset_index()
                    saldo_ind.columns = ['Indícios', 'Saldo']

                    fig = criar_grafico_barras(
                        saldo_ind, 'Indícios', 'Saldo',
                        'Saldo Credor por Número de Indícios',
                        tema
                    )
                    st.plotly_chart(fig, use_container_width=True, key="susp_saldo")

            criar_painel_exportacao(df_suspeitas)

    elif "Ranking" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>🏆 Ranking de Empresas</h1>",
                    unsafe_allow_html=True)

        top_n = filtros.get('top_n', 20)

        st.subheader(f"📊 Top {top_n} Empresas")

        tab1, tab2, tab3, tab4 = st.tabs([
            "💰 Maior Saldo",
            "⚠️ Maior Score",
            "💤 Mais Estagnadas",
            "📈 Maior Crescimento"
        ])

        with tab1:
            df_top_saldo = df_filtrado.nlargest(top_n, 'saldo_credor_atual')
            cols = ['nu_cnpj', 'nm_razao_social', 'saldo_credor_atual', 'qtde_ultimos_12m_iguais']

            col_score = get_col_name('score_risco', periodo)
            if col_score in df_top_saldo.columns:
                cols.append(col_score)

            st.dataframe(df_top_saldo[cols], use_container_width=True, hide_index=True)

            fig = criar_ranking_horizontal(
                df_top_saldo, 'nm_razao_social', 'saldo_credor_atual',
                f'Top {top_n} por Saldo Credor', top_n, tema
            )
            st.plotly_chart(fig, use_container_width=True, key="rank_saldo")

        with tab2:
            col_score = get_col_name('score_risco', periodo)
            if col_score in df_filtrado.columns:
                df_top_score = df_filtrado.nlargest(top_n, col_score)
                cols = ['nu_cnpj', 'nm_razao_social', col_score, 'saldo_credor_atual']

                st.dataframe(df_top_score[cols], use_container_width=True, hide_index=True)

                fig = criar_ranking_horizontal(
                    df_top_score, 'nm_razao_social', col_score,
                    f'Top {top_n} por Score de Risco', top_n, tema
                )
                st.plotly_chart(fig, use_container_width=True, key="rank_score")

        with tab3:
            df_top_estag = df_filtrado.nlargest(top_n, 'qtde_ultimos_12m_iguais')
            cols = ['nu_cnpj', 'nm_razao_social', 'qtde_ultimos_12m_iguais', 'saldo_credor_atual']

            st.dataframe(df_top_estag[cols], use_container_width=True, hide_index=True)

            fig = criar_ranking_horizontal(
                df_top_estag, 'nm_razao_social', 'qtde_ultimos_12m_iguais',
                f'Top {top_n} por Meses Estagnados', top_n, tema
            )
            st.plotly_chart(fig, use_container_width=True, key="rank_estag")

        with tab4:
            col_cresc = get_col_name('crescimento_saldo_percentual', periodo)
            if col_cresc in df_filtrado.columns:
                df_top_cresc = df_filtrado.nlargest(top_n, col_cresc)
                cols = ['nu_cnpj', 'nm_razao_social', col_cresc, 'saldo_credor_atual']

                st.dataframe(df_top_cresc[cols], use_container_width=True, hide_index=True)

                fig = criar_ranking_horizontal(
                    df_top_cresc, 'nm_razao_social', col_cresc,
                    f'Top {top_n} por Crescimento (%)', top_n, tema
                )
                st.plotly_chart(fig, use_container_width=True, key="rank_cresc")

    elif "Setorial" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>🏭 Análise Setorial</h1>",
                    unsafe_allow_html=True)

        df_setores = calcular_estatisticas_setoriais(dados, periodo)

        if df_setores.empty:
            st.warning("⚠️ Dados setoriais não disponíveis.")
        else:
            st.subheader("📊 Resumo Setorial")
            st.dataframe(df_setores, use_container_width=True, hide_index=True)

            st.divider()

            col1, col2 = st.columns(2)

            with col1:
                fig = criar_grafico_barras(
                    df_setores, 'Setor', 'Empresas',
                    'Empresas por Setor', tema
                )
                st.plotly_chart(fig, use_container_width=True, key="set_empresas")

            with col2:
                fig = criar_grafico_barras(
                    df_setores, 'Setor', 'Saldo Total',
                    'Saldo Credor por Setor', tema
                )
                st.plotly_chart(fig, use_container_width=True, key="set_saldo")

            st.subheader("🔍 Análise Detalhada")

            setor_selecionado = st.selectbox(
                "Selecione um setor:",
                df_setores['Setor'].tolist()
            )

            setor_map = {
                'TÊXTIL': 'textil',
                'METAL-MECÂNICO': 'metalmec',
                'TECNOLOGIA': 'tech'
            }

            setor_key = setor_map.get(setor_selecionado)

            if setor_key and setor_key in dados:
                df_setor = dados[setor_key]

                if not df_setor.empty:
                    kpis_setor = calcular_kpis_gerais(df_setor, periodo)

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Empresas", formatar_valor(kpis_setor['total_empresas'], 'numero'))

                    with col2:
                        st.metric("Saldo Total", formatar_valor(kpis_setor['saldo_total'], 'moeda'))

                    with col3:
                        st.metric("Score Médio", f"{kpis_setor['score_medio']:.1f}")

                    with col4:
                        st.metric("Críticos", formatar_valor(kpis_setor['criticos'], 'numero'))

                    st.subheader(f"Top 10 Empresas - {setor_selecionado}")
                    top_setor = df_setor.nlargest(10, 'saldo_credor_atual')

                    fig = criar_ranking_horizontal(
                        top_setor, 'nm_razao_social', 'saldo_credor_atual',
                        f'Top 10 {setor_selecionado} por Saldo', 10, tema
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"rank_{setor_key}")

    elif "Drill-Down" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>🔬 Drill-Down de Empresa</h1>",
                    unsafe_allow_html=True)

        cnpj_busca = st.text_input(
            "Digite o CNPJ (apenas números):",
            placeholder="00000000000000",
            max_chars=14
        )

        if cnpj_busca and len(cnpj_busca) >= 8:
            df_empresa = df_completo[df_completo['nu_cnpj'].astype(str).str.contains(cnpj_busca)]

            if df_empresa.empty:
                st.warning(f"⚠️ CNPJ {cnpj_busca} não encontrado.")
            else:
                empresa = df_empresa.iloc[0]

                st.markdown(
                    f"<div style='background: #1565c0; color: white; padding: 20px; "
                    f"border-radius: 10px; margin: 20px 0;'>"
                    f"<h2 style='margin: 0; color: white;'>{empresa.get('nm_razao_social', 'N/A')}</h2>"
                    f"<p style='margin: 5px 0 0 0;'>CNPJ: {formatar_cnpj(empresa['nu_cnpj'])}</p>"
                    f"</div>",
                    unsafe_allow_html=True
                )

                st.subheader("📋 Dados Principais")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    saldo = empresa.get('saldo_credor_atual', 0)
                    st.metric("Saldo Credor Atual", formatar_valor(saldo, 'moeda'))

                with col2:
                    estag = empresa.get('qtde_ultimos_12m_iguais', 0)
                    st.metric("Meses Estagnados", int(estag))

                with col3:
                    col_score = get_col_name('score_risco', periodo)
                    score = empresa.get(col_score, 0)
                    st.metric("Score de Risco", f"{score:.1f}")

                with col4:
                    col_class = get_col_name('classificacao_risco', periodo)
                    classif = empresa.get(col_class, 'N/A')
                    st.metric("Classificação", classif)

                st.divider()

                if 'qtde_indicios_fraude' in empresa:
                    st.subheader("⚠️ Indicadores de Fraude")

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        ind = int(empresa.get('qtde_indicios_fraude', 0))
                        st.metric("Indícios de Fraude", ind)

                    with col2:
                        susp = "SIM" if empresa.get('flag_empresa_suspeita', 0) == 1 else "NÃO"
                        st.metric("Empresa Suspeita", susp)

                    with col3:
                        canc = "SIM" if empresa.get('sn_cancelado_inex_inativ', 0) == 1 else "NÃO"
                        st.metric("Cancelada/Inex", canc)

                    with col4:
                        zer = "SIM" if empresa.get('flag_tem_declaracoes_zeradas', 0) == 1 else "NÃO"
                        st.metric("Decl. Zeradas", zer)

                st.divider()
                st.subheader("📊 Dados Completos")

                df_transp = pd.DataFrame({
                    'Campo': empresa.index,
                    'Valor': empresa.values
                })

                st.dataframe(df_transp, use_container_width=True, hide_index=True, height=400)

    elif "Machine Learning" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>🤖 Machine Learning & Priorização IA</h1>",
                    unsafe_allow_html=True)

        st.info(
            "**Sistema de Priorização Baseado em IA**\n\n"
            "Utiliza algoritmo de scoring combinando múltiplos fatores para "
            "identificar e priorizar casos de maior risco fiscal."
        )

        df_ml = calcular_score_ml(df_filtrado, periodo)

        if df_ml.empty:
            st.warning("⚠️ Sem dados para análise ML.")
        else:
            st.subheader("📊 Métricas de Priorização")

            col1, col2, col3, col4, col5 = st.columns(5)

            with col1:
                emergencial = len(df_ml[df_ml['nivel_alerta_ml'] == 'EMERGENCIAL'])
                st.metric("🔴 Emergencial", formatar_valor(emergencial, 'numero'))

            with col2:
                critico = len(df_ml[df_ml['nivel_alerta_ml'] == 'CRÍTICO'])
                st.metric("🟠 Crítico", formatar_valor(critico, 'numero'))

            with col3:
                alto = len(df_ml[df_ml['nivel_alerta_ml'] == 'ALTO'])
                st.metric("🟡 Alto", formatar_valor(alto, 'numero'))

            with col4:
                medio = len(df_ml[df_ml['nivel_alerta_ml'] == 'MÉDIO'])
                st.metric("🟢 Médio", formatar_valor(medio, 'numero'))

            with col5:
                score_ml_medio = df_ml['score_ml'].mean()
                st.metric("Score ML Médio", f"{score_ml_medio:.1f}")

            st.divider()

            st.subheader("📊 Distribuição de Níveis de Alerta")

            niveis = df_ml['nivel_alerta_ml'].value_counts().reset_index()
            niveis.columns = ['Nível', 'Quantidade']

            fig = criar_grafico_barras(
                niveis, 'Nível', 'Quantidade',
                'Distribuição por Nível de Alerta ML',
                tema
            )
            st.plotly_chart(fig, use_container_width=True, key="ml_niveis")

            st.subheader("🎯 Top 20 Casos Prioritários")

            df_top_ml = df_ml.nlargest(20, 'score_ml')

            cols = ['nu_cnpj', 'nm_razao_social', 'score_ml', 'nivel_alerta_ml',
                   'saldo_credor_atual', 'qtde_ultimos_12m_iguais']

            st.dataframe(df_top_ml[cols], use_container_width=True, hide_index=True)

            st.subheader("📈 Análise de Correlação")

            fig = criar_grafico_dispersao(
                df_ml.head(200), 'saldo_credor_atual', 'score_ml',
                'Score ML vs Saldo Credor (Top 200)',
                tema, 'nivel_alerta_ml', hover_data=['nm_razao_social']
            )
            st.plotly_chart(fig, use_container_width=True, key="ml_scatter")

            criar_painel_exportacao(df_ml)

    elif "Padrões de Abuso" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>⚠️ Detecção de Padrões de Abuso</h1>",
                    unsafe_allow_html=True)

        df_fraude = calcular_indicadores_fraude(df_filtrado, periodo)

        if df_fraude.empty:
            st.warning("⚠️ Sem dados para análise de padrões.")
        else:
            st.subheader("🔍 Padrões Detectados")

            padroes = {}

            if 'ind_crescimento_anormal' in df_fraude.columns:
                padroes['Crescimento Anormal (>200%)'] = (df_fraude['ind_crescimento_anormal'] == 1).sum()

            if 'ind_alto_estagnado' in df_fraude.columns:
                padroes['Alto Saldo + Estagnado'] = (df_fraude['ind_alto_estagnado'] == 1).sum()

            if 'ind_baixa_variacao' in df_fraude.columns:
                padroes['Baixa Variação + Alto Saldo'] = (df_fraude['ind_baixa_variacao'] == 1).sum()

            if 'ind_saldo_extremo' in df_fraude.columns:
                padroes['Saldo Extremo (>R$500K)'] = (df_fraude['ind_saldo_extremo'] == 1).sum()

            cols = st.columns(len(padroes))

            for i, (padrao, qtd) in enumerate(padroes.items()):
                with cols[i]:
                    st.metric(padrao, formatar_valor(qtd, 'numero'))

            st.divider()

            if padroes:
                df_padroes = pd.DataFrame(list(padroes.items()), columns=['Padrão', 'Quantidade'])

                fig = criar_grafico_barras(
                    df_padroes, 'Padrão', 'Quantidade',
                    'Padrões de Abuso Detectados',
                    tema, orientacao='h'
                )
                st.plotly_chart(fig, use_container_width=True, key="padroes_bar")

            if 'score_fraude_calculado' in df_fraude.columns:
                st.subheader("🚨 Empresas com Múltiplos Padrões")

                df_multi = df_fraude[df_fraude['score_fraude_calculado'] >= 2]

                if not df_multi.empty:
                    st.warning(f"⚠️ Encontradas **{len(df_multi):,}** empresas com 2+ padrões")

                    cols = ['nu_cnpj', 'nm_razao_social', 'score_fraude_calculado',
                           'saldo_credor_atual', 'qtde_ultimos_12m_iguais']

                    df_top_multi = df_multi.nlargest(20, 'score_fraude_calculado')[cols]
                    st.dataframe(df_top_multi, use_container_width=True, hide_index=True)

    elif "Inativas" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>💤 Empresas Inativas com Saldos</h1>",
                    unsafe_allow_html=True)

        df_inativas = df_filtrado[df_filtrado['qtde_ultimos_12m_iguais'] >= 12]

        if df_inativas.empty:
            st.success("✅ Nenhuma empresa inativa (12+ meses) encontrada.")
        else:
            st.warning(f"⚠️ **{len(df_inativas):,}** empresas inativas detectadas")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                saldo_inativo = df_inativas['saldo_credor_atual'].sum()
                st.metric("Saldo Total Inativo", formatar_valor(saldo_inativo, 'moeda'))

            with col2:
                meses_medio = df_inativas['qtde_ultimos_12m_iguais'].mean()
                st.metric("Meses Parados (Média)", f"{meses_medio:.0f}")

            with col3:
                max_meses = df_inativas['qtde_ultimos_12m_iguais'].max()
                st.metric("Máximo de Meses Parado", int(max_meses))

            with col4:
                if 'sn_cancelado_inex_inativ' in df_inativas.columns:
                    canc = (df_inativas['sn_cancelado_inex_inativ'] == 1).sum()
                    st.metric("Já Canceladas", formatar_valor(canc, 'numero'))

            st.divider()

            st.subheader("📊 Distribuição por Tempo Inativo")

            df_inativas['faixa_inatividade'] = pd.cut(
                df_inativas['qtde_ultimos_12m_iguais'],
                bins=[12, 24, 36, 48, 60, 100],
                labels=['12-24m', '25-36m', '37-48m', '49-60m', '60m+']
            )

            dist_inat = df_inativas['faixa_inatividade'].value_counts().reset_index()
            dist_inat.columns = ['Faixa', 'Quantidade']

            fig = criar_grafico_barras(
                dist_inat, 'Faixa', 'Quantidade',
                'Empresas por Faixa de Inatividade',
                tema
            )
            st.plotly_chart(fig, use_container_width=True, key="inat_faixas")

            st.subheader("📋 Top 20 Empresas Mais Inativas")

            df_top_inat = df_inativas.nlargest(20, 'qtde_ultimos_12m_iguais')
            cols = ['nu_cnpj', 'nm_razao_social', 'qtde_ultimos_12m_iguais',
                   'saldo_credor_atual']

            st.dataframe(df_top_inat[cols], use_container_width=True, hide_index=True)

            criar_painel_exportacao(df_inativas)

    elif "Noteiras" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>📋 Detecção de Empresas Noteiras</h1>",
                    unsafe_allow_html=True)

        if 'flag_empresa_noteira' in df_filtrado.columns:
            df_noteiras = df_filtrado[df_filtrado['flag_empresa_noteira'] == 1]

            if df_noteiras.empty:
                st.success("✅ Nenhuma empresa noteira identificada.")
            else:
                st.warning(f"⚠️ **{len(df_noteiras):,}** empresas noteiras detectadas")

                st.dataframe(
                    df_noteiras[['nu_cnpj', 'nm_razao_social', 'saldo_credor_atual']].head(50),
                    use_container_width=True,
                    hide_index=True
                )

                criar_painel_exportacao(df_noteiras)
        else:
            st.info("ℹ️ Dados de empresas noteiras não disponíveis.")

    elif "Zeradas" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>0️⃣ Análise de Declarações Zeradas</h1>",
                    unsafe_allow_html=True)

        if 'flag_tem_declaracoes_zeradas' in df_filtrado.columns:
            df_zeradas = df_filtrado[df_filtrado['flag_tem_declaracoes_zeradas'] == 1]

            if df_zeradas.empty:
                st.success("✅ Nenhuma empresa com declarações zeradas.")
            else:
                st.warning(f"⚠️ **{len(df_zeradas):,}** empresas com declarações zeradas")

                st.dataframe(
                    df_zeradas[['nu_cnpj', 'nm_razao_social', 'saldo_credor_atual']].head(50),
                    use_container_width=True,
                    hide_index=True
                )

                criar_painel_exportacao(df_zeradas)
        else:
            st.info("ℹ️ Dados de declarações zeradas não disponíveis.")

    elif "Sobre" in pagina_selecionada:
        st.markdown("<h1 class='main-header'>ℹ️ Sobre o Sistema CRED-CANCEL v3.0</h1>",
                    unsafe_allow_html=True)

        st.markdown("""
        ### 🎯 Visão Geral

        O **CRED-CANCEL v3.0** é um sistema integrado de análise fiscal desenvolvido para a
        Receita Estadual de Santa Catarina (SEF/SC), focado na detecção de fraudes em créditos
        acumulados de ICMS e identificação de empresas candidatas ao cancelamento de IE.

        ### 🔧 Recursos Principais

        - ✅ Análise de créditos em períodos de 12 e 60 meses
        - ✅ Machine Learning para priorização de casos
        - ✅ Detecção automática de padrões de abuso
        - ✅ Sistema de scoring multicritério
        - ✅ Exportação de dados e relatórios
        - ✅ Análise setorial especializada
        - ✅ Interface intuitiva e responsiva

        ### 📊 Tecnologias Utilizadas

        - **Frontend:** Streamlit
        - **Visualização:** Plotly
        - **Processamento:** Pandas, NumPy
        - **Banco de Dados:** Apache Impala (Hadoop)
        - **Autenticação:** LDAP + SSL

        ### 👨‍💻 Desenvolvimento

        **Desenvolvedor:** AFRE Tiago Severo
        **Versão:** 3.0.0 (Monolítico)
        **Data:** 2025
        **Órgão:** SEF/SC - Secretaria da Fazenda de Santa Catarina

        ### 📧 Suporte

        Para dúvidas ou sugestões, entre em contato com a equipe de desenvolvimento.

        ---

        *© 2025 SEF/SC - Todos os direitos reservados*
        """)

    # Footer
    st.divider()
    st.caption(
        f"CRED-CANCEL v3.0 (Monolítico) | Desenvolvido por AFRE Tiago Severo | "
        f"SEF/SC - {datetime.now().year} | "
        f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )


# =============================================================================
# EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    main()
