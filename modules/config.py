"""
Configurações centralizadas do sistema CRED-CANCEL v3.0
"""

import streamlit as st

# =============================================================================
# CREDENCIAIS E CONEXÃO
# =============================================================================

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

# =============================================================================
# TABELAS DO BANCO
# =============================================================================

TABELAS = {
    'completo': 'credito_dime_completo',
    'textil': 'credito_dime_textil',
    'metalmec': 'credito_dime_metalmec',
    'tech': 'credito_dime_tech'
}

# =============================================================================
# PERÍODOS DE ANÁLISE
# =============================================================================

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

# =============================================================================
# CONTEXTOS DE ANÁLISE
# =============================================================================

CONTEXTOS = {
    'cancelamento': {
        'icon': '📋',
        'title': 'Análise para Cancelamento de IE',
        'description': 'Identificação de empresas candidatas ao cancelamento de Inscrição Estadual',
        'criterios': [
            'Empresas com 6+ meses sem movimentação',
            'Omissão de declarações obrigatórias',
            'Declarações zeradas consecutivas',
            'CNAE não sujeito ao ICMS',
            'Empresas canceladas/inexistentes'
        ],
        'color': '#ef6c00'
    },
    'saldos_credores': {
        'icon': '💰',
        'title': 'Análise de Saldos Credores',
        'description': 'Verificação de créditos acumulados e padrões suspeitos',
        'criterios': [
            'Saldo credor alto e estagnado (6+ meses)',
            'Crescimento anormal de saldo (>200%)',
            'Saldos muito elevados (>R$ 500K)',
            'Baixa variação com alto crédito',
            'Crédito presumido suspeito'
        ],
        'color': '#2e7d32'
    },
    'ambos': {
        'icon': '🔄',
        'title': 'Análise Combinada',
        'description': 'Avaliação completa: Cancelamento + Saldos Credores',
        'criterios': [
            'Todos os critérios de cancelamento',
            'Todos os critérios de saldos credores',
            'Análise integrada de risco',
            'Priorização por impacto fiscal'
        ],
        'color': '#1976d2'
    }
}

# =============================================================================
# SETORES
# =============================================================================

SETORES = {
    'textil': {
        'nome': 'TÊXTIL',
        'flag': 'flag_setor_textil',
        'cor': '#e91e63',
        'icon': '🧵'
    },
    'metalmec': {
        'nome': 'METAL-MECÂNICO',
        'flag': 'flag_setor_metalmec',
        'cor': '#607d8b',
        'icon': '⚙️'
    },
    'tech': {
        'nome': 'TECNOLOGIA',
        'flag': 'flag_setor_tech',
        'cor': '#9c27b0',
        'icon': '💻'
    }
}

# =============================================================================
# CLASSIFICAÇÕES DE RISCO
# =============================================================================

CLASSIFICACOES_RISCO = {
    'CRÍTICO': {'cor': '#c62828', 'peso': 4},
    'ALTO': {'cor': '#ef6c00', 'peso': 3},
    'MÉDIO': {'cor': '#fbc02d', 'peso': 2},
    'BAIXO': {'cor': '#388e3c', 'peso': 1}
}

NIVEIS_ALERTA_ML = {
    'EMERGENCIAL': {'cor': '#3d0000', 'range': (85, 100), 'prioridade': 1},
    'CRÍTICO': {'cor': '#c62828', 'range': (70, 85), 'prioridade': 2},
    'ALTO': {'cor': '#ef6c00', 'range': (50, 70), 'prioridade': 3},
    'MÉDIO': {'cor': '#fbc02d', 'range': (30, 50), 'prioridade': 4},
    'BAIXO': {'cor': '#388e3c', 'range': (0, 30), 'prioridade': 5}
}

# =============================================================================
# PARÂMETROS DE MACHINE LEARNING
# =============================================================================

ML_PARAMS = {
    'peso_score': 0.4,
    'peso_saldo': 0.3,
    'peso_estagnacao': 0.3,
    'bins_alerta': [0, 30, 50, 70, 85, 100],
    'labels_alerta': ['BAIXO', 'MÉDIO', 'ALTO', 'CRÍTICO', 'EMERGENCIAL']
}

# =============================================================================
# THRESHOLDS E LIMITES
# =============================================================================

THRESHOLDS = {
    'saldo_alto': 500000,
    'saldo_medio': 50000,
    'crescimento_anormal': 200,  # percentual
    'meses_estagnado_min': 6,
    'meses_estagnado_critico': 12,
    'indicios_fraude_critico': 5,
    'desvio_padrao_baixo': 1000,
    'cv_baixo': 0.1  # coeficiente de variação
}

# =============================================================================
# CACHE E PERFORMANCE
# =============================================================================

CACHE_CONFIG = {
    'ttl_dados': 3600,  # 1 hora
    'ttl_metricas': 1800,  # 30 minutos
    'max_entries': 100
}

# =============================================================================
# TEMAS VISUAIS
# =============================================================================

TEMAS_PLOTLY = {
    'Claro': 'plotly_white',
    'Escuro': 'plotly_dark',
    'Padrão': 'plotly'
}

# =============================================================================
# MAPEAMENTO DE COLUNAS POR PERÍODO
# =============================================================================

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

# =============================================================================
# COLUNAS DE ENRIQUECIMENTO
# =============================================================================

COLUNAS_ENRIQUECIMENTO = {
    'fraude': [
        'flag_empresa_suspeita',
        'qtde_indicios_fraude',
        'flag_tem_declaracoes_zeradas',
        'flag_tem_omissoes',
        'flag_empresa_noteira',
        'score_suspeita'
    ],
    'status': [
        'sn_cancelado_inex_inativ',
        'flag_tem_pagamentos'
    ],
    'temporal': [
        'qtde_ultimos_12m_iguais',
        'qtde_ultimos_60m_iguais'
    ]
}

# =============================================================================
# CONFIGURAÇÕES DE PÁGINA
# =============================================================================

PAGE_CONFIG = {
    'page_title': 'CRED-CANCEL v3.0 - Sistema Integrado de Análise Fiscal',
    'page_icon': '💰',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
}

# =============================================================================
# ESTILOS CSS
# =============================================================================

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

    .alert-positivo {
        background-color: #e8f5e9;
        border-left: 5px solid #2e7d32;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .alert-emergencial {
        background-color: #3d0000;
        border-left: 5px solid #ff0000;
        color: white;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        font-weight: bold;
    }

    .info-box {
        background-color: #e3f2fd;
        border-left: 4px solid #1976d2;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }

    /* CARDS PERSONALIZADOS */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
    }

    .insight-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
    }

    .success-card {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
    }

    /* TABELAS */
    .stDataFrame {
        font-size: 0.9rem;
        border-radius: 10px;
    }

    /* SIDEBAR */
    .css-1d391kg {
        background-color: #f8f9fa;
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

# =============================================================================
# MENSAGENS E TEXTOS
# =============================================================================

MENSAGENS = {
    'bem_vindo': '🎯 Bem-vindo ao CRED-CANCEL v3.0',
    'carregando': '⏳ Carregando dados...',
    'erro_conexao': '❌ Erro ao conectar ao banco de dados',
    'erro_dados': '❌ Erro ao carregar dados',
    'sucesso_export': '✅ Dados exportados com sucesso!',
    'sem_dados': '⚠️ Nenhum dado encontrado com os filtros aplicados',
    'analise_completa': '✅ Análise completa finalizada!'
}
