"""
Módulo de filtros do sistema CRED-CANCEL v3.0
"""

import streamlit as st
import pandas as pd
from typing import Dict
from .config import CONTEXTOS, PERIODOS, TEMAS_PLOTLY
from .utils import get_col_name


def criar_filtros_sidebar(dados: Dict) -> Dict:
    """
    Cria painel de filtros na sidebar.

    Args:
        dados: Dicionário com DataFrames

    Returns:
        Dicionário com filtros selecionados
    """
    filtros = {}

    # =============================================================================
    # CONTEXTO DA ANÁLISE
    # =============================================================================
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

    # =============================================================================
    # PERÍODO DE ANÁLISE
    # =============================================================================
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

    # =============================================================================
    # FILTROS GLOBAIS
    # =============================================================================
    with st.sidebar.expander("🔍 Filtros Globais", expanded=True):

        # Classificações de Risco
        filtros['classificacoes'] = st.multiselect(
            "Classificações de Risco:",
            ['CRÍTICO', 'ALTO', 'MÉDIO', 'BAIXO'],
            default=['CRÍTICO', 'ALTO', 'MÉDIO', 'BAIXO'],
            help="Filtrar por nível de risco"
        )

        # Saldo Mínimo
        filtros['saldo_minimo'] = st.number_input(
            "Saldo Credor Mínimo (R$):",
            min_value=0,
            max_value=10000000,
            value=0,
            step=10000,
            format="%d",
            help="Saldo credor mínimo para exibir"
        )

        # Meses Estagnados
        filtros['meses_iguais_min'] = st.slider(
            "Meses Estagnados (mínimo):",
            min_value=0,
            max_value=60,
            value=0,
            help="Número mínimo de meses com saldo igual"
        )

        # GERFE
        df_completo = dados.get('completo', pd.DataFrame())
        if not df_completo.empty and 'nm_gerfe' in df_completo.columns:
            gerfes_disponiveis = sorted(df_completo['nm_gerfe'].dropna().unique().tolist())
            gerfes = ['TODAS'] + gerfes_disponiveis
            filtros['gerfe'] = st.selectbox(
                "GERFE (Gerência Regional):",
                gerfes,
                help="Filtrar por gerência regional"
            )

    # =============================================================================
    # FILTROS DE FRAUDE
    # =============================================================================
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

    # =============================================================================
    # FILTROS AVANÇADOS
    # =============================================================================
    with st.sidebar.expander("⚙️ Filtros Avançados", expanded=False):

        # Score Mínimo
        filtros['score_min'] = st.slider(
            "Score de Risco Mínimo:",
            min_value=0,
            max_value=100,
            value=0,
            help="Score mínimo de risco"
        )

        # Crescimento
        filtros['crescimento_min'] = st.number_input(
            "Crescimento Mínimo (%):",
            min_value=-1000,
            max_value=10000,
            value=-1000,
            step=50,
            help="Crescimento percentual mínimo do saldo"
        )

    # =============================================================================
    # VISUALIZAÇÃO
    # =============================================================================
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
    """
    Aplica filtros no DataFrame.

    Args:
        df: DataFrame original
        filtros: Dicionário com filtros

    Returns:
        DataFrame filtrado
    """
    if df.empty:
        return df

    df_filtrado = df.copy()

    # Obter período
    periodo = filtros.get('periodo', '12m')
    col_class = get_col_name('classificacao_risco', periodo)
    col_score = get_col_name('score_risco', periodo)
    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)

    # =============================================================================
    # FILTROS BÁSICOS
    # =============================================================================

    # Classificações
    if filtros.get('classificacoes') and len(filtros['classificacoes']) < 4:
        if col_class in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado[col_class].isin(filtros['classificacoes'])]

    # Saldo mínimo
    if filtros.get('saldo_minimo', 0) > 0:
        df_filtrado = df_filtrado[df_filtrado['saldo_credor_atual'] >= filtros['saldo_minimo']]

    # Meses estagnados
    if filtros.get('meses_iguais_min', 0) > 0:
        df_filtrado = df_filtrado[df_filtrado['qtde_ultimos_12m_iguais'] >= filtros['meses_iguais_min']]

    # GERFE
    if filtros.get('gerfe') and filtros['gerfe'] != 'TODAS':
        if 'nm_gerfe' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['nm_gerfe'] == filtros['gerfe']]

    # =============================================================================
    # FILTROS DE FRAUDE
    # =============================================================================

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

    # =============================================================================
    # FILTROS AVANÇADOS
    # =============================================================================

    if filtros.get('score_min', 0) > 0 and col_score in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado[col_score] >= filtros['score_min']]

    if filtros.get('crescimento_min', -1000) > -1000 and col_cresc in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado[col_cresc] >= filtros['crescimento_min']]

    # =============================================================================
    # FILTROS CONTEXTUAIS
    # =============================================================================

    contexto = filtros.get('contexto', 'ambos')

    if contexto == 'cancelamento':
        df_filtrado = _aplicar_filtro_cancelamento(df_filtrado)

    elif contexto == 'saldos_credores':
        df_filtrado = _aplicar_filtro_saldos(df_filtrado, periodo)

    return df_filtrado


def _aplicar_filtro_cancelamento(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica lógica de filtro para cancelamento."""
    df['flag_cancelamento'] = (
        (df['qtde_ultimos_12m_iguais'] >= 6) |
        (df.get('flag_tem_omissoes', 0) == 1) |
        (df.get('flag_tem_declaracoes_zeradas', 0) == 1) |
        (df.get('sn_cancelado_inex_inativ', 0) == 1)
    )
    return df


def _aplicar_filtro_saldos(df: pd.DataFrame, periodo: str) -> pd.DataFrame:
    """Aplica lógica de filtro para saldos credores."""
    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)

    condicao_crescimento = (df[col_cresc] > 200) if col_cresc in df.columns else False

    df['flag_saldo_suspeito'] = (
        ((df['saldo_credor_atual'] > 50000) &
         (df['qtde_ultimos_12m_iguais'] >= 6)) |
        condicao_crescimento |
        (df['saldo_credor_atual'] > 500000)
    )
    return df


def get_resumo_filtros(filtros: Dict, df_original: pd.DataFrame,
                      df_filtrado: pd.DataFrame) -> Dict:
    """
    Gera resumo dos filtros aplicados.

    Args:
        filtros: Filtros aplicados
        df_original: DataFrame original
        df_filtrado: DataFrame após filtros

    Returns:
        Dicionário com resumo
    """
    total_original = len(df_original)
    total_filtrado = len(df_filtrado)
    pct_mantido = (total_filtrado / total_original * 100) if total_original > 0 else 0

    resumo = {
        'total_original': total_original,
        'total_filtrado': total_filtrado,
        'removidos': total_original - total_filtrado,
        'pct_mantido': pct_mantido,
        'contexto': filtros.get('contexto', 'ambos'),
        'periodo': filtros.get('periodo', '12m'),
        'filtros_ativos': []
    }

    # Identificar filtros ativos
    if filtros.get('saldo_minimo', 0) > 0:
        resumo['filtros_ativos'].append(f"Saldo mínimo: R$ {filtros['saldo_minimo']:,}")

    if filtros.get('meses_iguais_min', 0) > 0:
        resumo['filtros_ativos'].append(f"Meses estagnados: ≥ {filtros['meses_iguais_min']}")

    if filtros.get('gerfe') and filtros['gerfe'] != 'TODAS':
        resumo['filtros_ativos'].append(f"GERFE: {filtros['gerfe']}")

    if filtros.get('apenas_suspeitas'):
        resumo['filtros_ativos'].append("Apenas suspeitas")

    if filtros.get('apenas_canceladas'):
        resumo['filtros_ativos'].append("Apenas canceladas")

    if filtros.get('min_indicios', 0) > 0:
        resumo['filtros_ativos'].append(f"Indícios: ≥ {filtros['min_indicios']}")

    return resumo
