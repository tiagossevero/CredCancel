"""
CRED-CANCEL v3.0 - Sistema Integrado de Análise Fiscal
Receita Estadual de Santa Catarina - SEF/SC

Aplicação Streamlit refatorada e otimizada
Desenvolvido por: AFRE Tiago Severo
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import warnings

# Importar módulos do sistema
from modules.config import PAGE_CONFIG, CSS_STYLES, MENSAGENS, CONTEXTOS, PERIODOS
from modules.auth import check_password, logout
from modules.database import get_impala_engine, carregar_dados_creditos
from modules.utils import formatar_valor, formatar_cnpj, get_col_name
from modules.metrics import (
    calcular_kpis_gerais, calcular_estatisticas_setoriais,
    calcular_score_ml, calcular_indicadores_fraude,
    calcular_metricas_comparativas, calcular_concentracao_risco,
    calcular_tendencias
)
from modules.filters import criar_filtros_sidebar, aplicar_filtros, get_resumo_filtros
from modules.visualizations import (
    criar_grafico_pizza, criar_grafico_barras, criar_grafico_dispersao,
    criar_ranking_horizontal, criar_grafico_comparativo_dual,
    criar_heatmap, criar_gauge, COLOR_MAP_RISCO
)
from modules.exporters import criar_painel_exportacao

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURAÇÃO DA PÁGINA
# =============================================================================

st.set_page_config(**PAGE_CONFIG)

# Aplicar estilos CSS
st.markdown(CSS_STYLES, unsafe_allow_html=True)

# =============================================================================
# AUTENTICAÇÃO
# =============================================================================

check_password()

# =============================================================================
# HEADER PRINCIPAL
# =============================================================================

st.markdown(
    "<h1 style='text-align: center; color: #1565c0; margin-bottom: 0;'>"
    "💰 CRED-CANCEL v3.0</h1>"
    "<p style='text-align: center; color: #666; margin-top: 0;'>"
    "Sistema Integrado de Análise Fiscal - SEF/SC</p>",
    unsafe_allow_html=True
)

# =============================================================================
# CARREGAR DADOS
# =============================================================================

# Engine
engine = get_impala_engine()

if engine is None:
    st.error("❌ Não foi possível conectar ao banco de dados.")
    st.stop()

# Carregar dados
with st.spinner("⏳ Carregando dados do Impala..."):
    dados = carregar_dados_creditos(engine)

if not dados or all(df.empty for df in dados.values()):
    st.error("❌ Nenhum dado foi carregado. Verifique a conexão.")
    st.stop()

# DataFrame principal
df_completo = dados.get('completo', pd.DataFrame())

if df_completo.empty:
    st.error("❌ DataFrame principal vazio.")
    st.stop()

# =============================================================================
# SIDEBAR - FILTROS E NAVEGAÇÃO
# =============================================================================

st.sidebar.title("⚙️ Configurações")

# Criar filtros
filtros = criar_filtros_sidebar(dados)

# Aplicar filtros
df_filtrado = aplicar_filtros(df_completo, filtros)

# Resumo de filtros
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
    "🔄 Reforma Tributária",
    "📋 Empresas Noteiras",
    "0️⃣ Declarações Zeradas",
    "🚨 Alertas Automáticos",
    "📖 Guia de Cancelamento",
    "ℹ️ Sobre o Sistema"
]

pagina_selecionada = st.sidebar.radio("Escolha uma página:", menu_opcoes, label_visibility="collapsed")

# =============================================================================
# OBTER PERÍODO E CONTEXTO
# =============================================================================

periodo = filtros.get('periodo', '12m')
contexto = filtros.get('contexto', 'ambos')
tema = filtros.get('tema', 'plotly_white')

# =============================================================================
# BANNER DO PERÍODO
# =============================================================================

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

# -----------------------------------------------------------------------------
# 🏠 DASHBOARD EXECUTIVO
# -----------------------------------------------------------------------------

if "Dashboard Executivo" in pagina_selecionada:
    contexto_info = CONTEXTOS.get(contexto, CONTEXTOS['ambos'])

    st.markdown(f"<h1 class='main-header'>{contexto_info['icon']} {contexto_info['title']}</h1>",
                unsafe_allow_html=True)

    # Calcular KPIs
    kpis = calcular_kpis_gerais(df_filtrado, periodo)

    # KPIs Principais
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

    # Linha 2 - Indicadores de Contexto
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

    # Gráficos
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

    # Concentração de Risco
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

        # Gráfico de ranking
        saldo_gerfe = df_filtrado.groupby('nm_gerfe')['saldo_credor_atual'].agg(['sum', 'count']).reset_index()
        saldo_gerfe.columns = ['GERFE', 'Saldo Total', 'Quantidade']

        fig = criar_ranking_horizontal(
            saldo_gerfe, 'GERFE', 'Saldo Total',
            'Top 10 GERFEs por Saldo Credor', 10, tema
        )
        st.plotly_chart(fig, use_container_width=True, key="exec_ranking_gerfe")

    # Painel de Exportação
    st.divider()
    criar_painel_exportacao(df_filtrado, kpis)

# -----------------------------------------------------------------------------
# 📊 ANÁLISE COMPARATIVA 12M VS 60M
# -----------------------------------------------------------------------------

elif "Comparativa" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>📊 Análise Comparativa: 12m vs 60m</h1>",
                unsafe_allow_html=True)

    # Calcular métricas comparativas
    df_comp = calcular_metricas_comparativas(df_filtrado)

    # KPIs Comparativos
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

    # Gráficos comparativos
    st.subheader("📊 Visualizações Comparativas")

    # Distribuição de classificações
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

    # Mudanças de classificação
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

# -----------------------------------------------------------------------------
# 🔍 ANÁLISE DE SUSPEITAS
# -----------------------------------------------------------------------------

elif "Suspeitas" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🔍 Análise de Empresas Suspeitas</h1>",
                unsafe_allow_html=True)

    # Filtrar apenas suspeitas
    if 'flag_empresa_suspeita' in df_filtrado.columns:
        df_suspeitas = df_filtrado[df_filtrado['flag_empresa_suspeita'] == 1]
    else:
        df_suspeitas = pd.DataFrame()

    if df_suspeitas.empty:
        st.warning("⚠️ Nenhuma empresa suspeita encontrada com os filtros atuais.")
    else:
        st.success(f"✅ Encontradas **{len(df_suspeitas):,}** empresas suspeitas")

        # KPIs de suspeitas
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

        # Top suspeitas
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

        # Gráficos
        st.subheader("📊 Análises Visuais")

        if 'qtde_indicios_fraude' in df_suspeitas.columns:
            col1, col2 = st.columns(2)

            with col1:
                # Distribuição de indícios
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
                # Saldo por indícios
                saldo_ind = df_suspeitas.groupby('qtde_indicios_fraude')['saldo_credor_atual'].sum().reset_index()
                saldo_ind.columns = ['Indícios', 'Saldo']

                fig = criar_grafico_barras(
                    saldo_ind, 'Indícios', 'Saldo',
                    'Saldo Credor por Número de Indícios',
                    tema
                )
                st.plotly_chart(fig, use_container_width=True, key="susp_saldo")

        criar_painel_exportacao(df_suspeitas)

# -----------------------------------------------------------------------------
# 🏆 RANKING DE EMPRESAS
# -----------------------------------------------------------------------------

elif "Ranking" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🏆 Ranking de Empresas</h1>",
                unsafe_allow_html=True)

    top_n = filtros.get('top_n', 20)

    st.subheader(f"📊 Top {top_n} Empresas")

    # Tabs para diferentes rankings
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

# -----------------------------------------------------------------------------
# 🏭 ANÁLISE SETORIAL
# -----------------------------------------------------------------------------

elif "Setorial" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🏭 Análise Setorial</h1>",
                unsafe_allow_html=True)

    # Calcular estatísticas setoriais
    df_setores = calcular_estatisticas_setoriais(dados, periodo)

    if df_setores.empty:
        st.warning("⚠️ Dados setoriais não disponíveis.")
    else:
        st.subheader("📊 Resumo Setorial")
        st.dataframe(df_setores, use_container_width=True, hide_index=True)

        st.divider()

        # Gráficos setoriais
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

        # Análise detalhada por setor
        st.subheader("🔍 Análise Detalhada")

        setor_selecionado = st.selectbox(
            "Selecione um setor:",
            df_setores['Setor'].tolist()
        )

        # Mapear setor para chave
        setor_map = {
            'TÊXTIL': 'textil',
            'METAL-MECÂNICO': 'metalmec',
            'TECNOLOGIA': 'tech'
        }

        setor_key = setor_map.get(setor_selecionado)

        if setor_key and setor_key in dados:
            df_setor = dados[setor_key]

            if not df_setor.empty:
                # KPIs do setor
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

                # Top empresas do setor
                st.subheader(f"Top 10 Empresas - {setor_selecionado}")
                top_setor = df_setor.nlargest(10, 'saldo_credor_atual')

                fig = criar_ranking_horizontal(
                    top_setor, 'nm_razao_social', 'saldo_credor_atual',
                    f'Top 10 {setor_selecionado} por Saldo', 10, tema
                )
                st.plotly_chart(fig, use_container_width=True, key=f"rank_{setor_key}")

# -----------------------------------------------------------------------------
# 🔬 DRILL-DOWN DE EMPRESA
# -----------------------------------------------------------------------------

elif "Drill-Down" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🔬 Drill-Down de Empresa</h1>",
                unsafe_allow_html=True)

    # Input CNPJ
    cnpj_busca = st.text_input(
        "Digite o CNPJ (apenas números):",
        placeholder="00000000000000",
        max_chars=14
    )

    if cnpj_busca and len(cnpj_busca) >= 8:
        # Buscar empresa
        df_empresa = df_completo[df_completo['nu_cnpj'].astype(str).str.contains(cnpj_busca)]

        if df_empresa.empty:
            st.warning(f"⚠️ CNPJ {cnpj_busca} não encontrado.")
        else:
            empresa = df_empresa.iloc[0]

            # Header da empresa
            st.markdown(
                f"<div style='background: #1565c0; color: white; padding: 20px; "
                f"border-radius: 10px; margin: 20px 0;'>"
                f"<h2 style='margin: 0; color: white;'>{empresa.get('nm_razao_social', 'N/A')}</h2>"
                f"<p style='margin: 5px 0 0 0;'>CNPJ: {formatar_cnpj(empresa['nu_cnpj'])}</p>"
                f"</div>",
                unsafe_allow_html=True
            )

            # Dados principais
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

            # Indicadores de fraude
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

            # Dados completos
            st.divider()
            st.subheader("📊 Dados Completos")

            # Transpor para visualização vertical
            df_transp = pd.DataFrame({
                'Campo': empresa.index,
                'Valor': empresa.values
            })

            st.dataframe(df_transp, use_container_width=True, hide_index=True, height=400)

# -----------------------------------------------------------------------------
# 🤖 MACHINE LEARNING & IA
# -----------------------------------------------------------------------------

elif "Machine Learning" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🤖 Machine Learning & Priorização IA</h1>",
                unsafe_allow_html=True)

    st.info(
        "**Sistema de Priorização Baseado em IA**\n\n"
        "Utiliza algoritmo de scoring combinando múltiplos fatores para "
        "identificar e priorizar casos de maior risco fiscal."
    )

    # Calcular scores ML
    df_ml = calcular_score_ml(df_filtrado, periodo)

    if df_ml.empty:
        st.warning("⚠️ Sem dados para análise ML.")
    else:
        # KPIs ML
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

        # Distribuição de níveis
        st.subheader("📊 Distribuição de Níveis de Alerta")

        niveis = df_ml['nivel_alerta_ml'].value_counts().reset_index()
        niveis.columns = ['Nível', 'Quantidade']

        fig = criar_grafico_barras(
            niveis, 'Nível', 'Quantidade',
            'Distribuição por Nível de Alerta ML',
            tema
        )
        st.plotly_chart(fig, use_container_width=True, key="ml_niveis")

        # Top prioritários
        st.subheader("🎯 Top 20 Casos Prioritários")

        df_top_ml = df_ml.nlargest(20, 'score_ml')

        cols = ['nu_cnpj', 'nm_razao_social', 'score_ml', 'nivel_alerta_ml',
               'saldo_credor_atual', 'qtde_ultimos_12m_iguais']

        st.dataframe(df_top_ml[cols], use_container_width=True, hide_index=True)

        # Gráfico de dispersão
        st.subheader("📈 Análise de Correlação")

        fig = criar_grafico_dispersao(
            df_ml.head(200), 'saldo_credor_atual', 'score_ml',
            'Score ML vs Saldo Credor (Top 200)',
            tema, 'nivel_alerta_ml', hover_data=['nm_razao_social']
        )
        st.plotly_chart(fig, use_container_width=True, key="ml_scatter")

        criar_painel_exportacao(df_ml)

# -----------------------------------------------------------------------------
# ⚠️ PADRÕES DE ABUSO
# -----------------------------------------------------------------------------

elif "Padrões de Abuso" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>⚠️ Detecção de Padrões de Abuso</h1>",
                unsafe_allow_html=True)

    # Calcular indicadores de fraude
    df_fraude = calcular_indicadores_fraude(df_filtrado, periodo)

    if df_fraude.empty:
        st.warning("⚠️ Sem dados para análise de padrões.")
    else:
        st.subheader("🔍 Padrões Detectados")

        # Contar padrões
        padroes = {}

        if 'ind_crescimento_anormal' in df_fraude.columns:
            padroes['Crescimento Anormal (>200%)'] = (df_fraude['ind_crescimento_anormal'] == 1).sum()

        if 'ind_alto_estagnado' in df_fraude.columns:
            padroes['Alto Saldo + Estagnado'] = (df_fraude['ind_alto_estagnado'] == 1).sum()

        if 'ind_baixa_variacao' in df_fraude.columns:
            padroes['Baixa Variação + Alto Saldo'] = (df_fraude['ind_baixa_variacao'] == 1).sum()

        if 'ind_saldo_extremo' in df_fraude.columns:
            padroes['Saldo Extremo (>R$500K)'] = (df_fraude['ind_saldo_extremo'] == 1).sum()

        # Exibir padrões
        cols = st.columns(len(padroes))

        for i, (padrao, qtd) in enumerate(padroes.items()):
            with cols[i]:
                st.metric(padrao, formatar_valor(qtd, 'numero'))

        st.divider()

        # Gráfico de padrões
        if padroes:
            df_padroes = pd.DataFrame(list(padroes.items()), columns=['Padrão', 'Quantidade'])

            fig = criar_grafico_barras(
                df_padroes, 'Padrão', 'Quantidade',
                'Padrões de Abuso Detectados',
                tema, orientacao='h'
            )
            st.plotly_chart(fig, use_container_width=True, key="padroes_bar")

        # Empresas com múltiplos padrões
        if 'score_fraude_calculado' in df_fraude.columns:
            st.subheader("🚨 Empresas com Múltiplos Padrões")

            df_multi = df_fraude[df_fraude['score_fraude_calculado'] >= 2]

            if not df_multi.empty:
                st.warning(f"⚠️ Encontradas **{len(df_multi):,}** empresas com 2+ padrões")

                cols = ['nu_cnpj', 'nm_razao_social', 'score_fraude_calculado',
                       'saldo_credor_atual', 'qtde_ultimos_12m_iguais']

                df_top_multi = df_multi.nlargest(20, 'score_fraude_calculado')[cols]
                st.dataframe(df_top_multi, use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# 💤 EMPRESAS INATIVAS
# -----------------------------------------------------------------------------

elif "Inativas" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>💤 Empresas Inativas com Saldos</h1>",
                unsafe_allow_html=True)

    # Filtrar inativas (12+ meses estagnadas)
    df_inativas = df_filtrado[df_filtrado['qtde_ultimos_12m_iguais'] >= 12]

    if df_inativas.empty:
        st.success("✅ Nenhuma empresa inativa (12+ meses) encontrada.")
    else:
        st.warning(f"⚠️ **{len(df_inativas):,}** empresas inativas detectadas")

        # KPIs
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

        # Distribuição por faixa
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

        # Top inativas
        st.subheader("📋 Top 20 Empresas Mais Inativas")

        df_top_inat = df_inativas.nlargest(20, 'qtde_ultimos_12m_iguais')
        cols = ['nu_cnpj', 'nm_razao_social', 'qtde_ultimos_12m_iguais',
               'saldo_credor_atual']

        st.dataframe(df_top_inat[cols], use_container_width=True, hide_index=True)

        criar_painel_exportacao(df_inativas)

# -----------------------------------------------------------------------------
# OUTRAS PÁGINAS (simplificadas)
# -----------------------------------------------------------------------------

elif "Reforma Tributária" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🔄 Impacto da Reforma Tributária</h1>",
                unsafe_allow_html=True)
    st.info("🚧 Funcionalidade em desenvolvimento. Aguarde atualizações.")

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

elif "Alertas" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>🚨 Sistema de Alertas Automáticos</h1>",
                unsafe_allow_html=True)
    st.info("🚧 Funcionalidade em desenvolvimento. Sistema de alertas em tempo real.")

elif "Guia" in pagina_selecionada:
    st.markdown("<h1 class='main-header'>📖 Guia de Cancelamento de IE</h1>",
                unsafe_allow_html=True)
    st.info("🚧 Funcionalidade em desenvolvimento. Guia completo de procedimentos.")

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
    **Versão:** 3.0.0
    **Data:** 2025
    **Órgão:** SEF/SC - Secretaria da Fazenda de Santa Catarina

    ### 📧 Suporte

    Para dúvidas ou sugestões, entre em contato com a equipe de desenvolvimento.

    ---

    *© 2025 SEF/SC - Todos os direitos reservados*
    """)

# =============================================================================
# FOOTER
# =============================================================================

st.divider()
st.caption(
    f"CRED-CANCEL v3.0 | Desenvolvido por AFRE Tiago Severo | "
    f"SEF/SC - {datetime.now().year} | "
    f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
