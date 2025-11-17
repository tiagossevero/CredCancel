"""
Módulo de cálculos e métricas do sistema CRED-CANCEL v3.0
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from .utils import get_col_name, safe_divide
from .config import ML_PARAMS, THRESHOLDS


def calcular_kpis_gerais(df: pd.DataFrame, periodo: str = '12m') -> Dict:
    """
    Calcula KPIs principais do sistema.

    Args:
        df: DataFrame com dados
        periodo: '12m', '60m' ou 'comparativo'

    Returns:
        Dicionário com KPIs
    """
    if df.empty:
        return {k: 0 for k in [
            'total_empresas', 'total_grupos', 'saldo_total', 'score_medio',
            'score_combinado_medio', 'criticos', 'altos', 'medios', 'baixos',
            'congelados_12m', 'crescimento_medio', 'cp_total',
            'empresas_suspeitas', 'empresas_canceladas',
            'empresas_5plus_indicios', 'saldo_medio_empresa',
            'saldo_mediano', 'desvio_padrao_saldo'
        ]}

    # Obter nomes corretos das colunas
    col_score = get_col_name('score_risco', periodo)
    col_classificacao = get_col_name('classificacao_risco', periodo)
    col_crescimento = get_col_name('crescimento_saldo_percentual', periodo)
    col_cp = get_col_name('vl_credito_presumido', periodo)
    col_score_comb = get_col_name('score_risco_combinado', periodo)

    kpis = {
        # Contagens básicas
        'total_empresas': df['nu_cnpj'].nunique(),
        'total_grupos': df['nu_cnpj_grupo'].nunique() if 'nu_cnpj_grupo' in df.columns else 0,

        # Saldos
        'saldo_total': float(df['saldo_credor_atual'].sum()),
        'saldo_medio_empresa': float(df['saldo_credor_atual'].mean()),
        'saldo_mediano': float(df['saldo_credor_atual'].median()),
        'desvio_padrao_saldo': float(df['saldo_credor_atual'].std()),

        # Scores
        'score_medio': float(df[col_score].mean()) if col_score in df.columns else 0,
        'score_combinado_medio': float(df[col_score_comb].mean()) if col_score_comb in df.columns else 0,

        # Classificações
        'criticos': len(df[df[col_classificacao] == 'CRÍTICO']) if col_classificacao in df.columns else 0,
        'altos': len(df[df[col_classificacao] == 'ALTO']) if col_classificacao in df.columns else 0,
        'medios': len(df[df[col_classificacao] == 'MÉDIO']) if col_classificacao in df.columns else 0,
        'baixos': len(df[df[col_classificacao] == 'BAIXO']) if col_classificacao in df.columns else 0,

        # Estagnação
        'congelados_12m': len(df[df['qtde_ultimos_12m_iguais'] >= 12]),
        'congelados_6m': len(df[df['qtde_ultimos_12m_iguais'] >= 6]),

        # Crescimento
        'crescimento_medio': float(df[col_crescimento].mean()) if col_crescimento in df.columns else 0,

        # Crédito Presumido
        'cp_total': float(df[col_cp].sum()) if col_cp in df.columns else 0,

        # Fraude
        'empresas_suspeitas': len(df[df['flag_empresa_suspeita'] == 1]) if 'flag_empresa_suspeita' in df.columns else 0,
        'empresas_canceladas': len(df[df['sn_cancelado_inex_inativ'] == 1]) if 'sn_cancelado_inex_inativ' in df.columns else 0,
        'empresas_5plus_indicios': len(df[df['qtde_indicios_fraude'] >= 5]) if 'qtde_indicios_fraude' in df.columns else 0,
    }

    # Percentuais
    if kpis['total_empresas'] > 0:
        kpis['pct_criticos'] = (kpis['criticos'] / kpis['total_empresas']) * 100
        kpis['pct_altos'] = (kpis['altos'] / kpis['total_empresas']) * 100
        kpis['pct_suspeitas'] = (kpis['empresas_suspeitas'] / kpis['total_empresas']) * 100
        kpis['pct_congelados'] = (kpis['congelados_12m'] / kpis['total_empresas']) * 100

    return kpis


def calcular_estatisticas_setoriais(dados: Dict, periodo: str = '12m') -> pd.DataFrame:
    """
    Calcula estatísticas dos setores.

    Args:
        dados: Dicionário com DataFrames por setor
        periodo: '12m', '60m' ou 'comparativo'

    Returns:
        DataFrame com estatísticas setoriais
    """
    setores = []

    col_score = get_col_name('score_risco', periodo)
    col_class = get_col_name('classificacao_risco', periodo)

    for setor_key, setor_nome, flag_col in [
        ('textil', 'TÊXTIL', 'flag_setor_textil'),
        ('metalmec', 'METAL-MECÂNICO', 'flag_setor_metalmec'),
        ('tech', 'TECNOLOGIA', 'flag_setor_tech')
    ]:
        df = dados.get(setor_key, pd.DataFrame())

        if df.empty:
            continue

        # Filtrar apenas empresas ativas do setor
        if flag_col in df.columns:
            df_ativo = df[df[flag_col] == 1]
        else:
            df_ativo = df

        if df_ativo.empty:
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

        setores.append(setor_info)

    return pd.DataFrame(setores)


def calcular_score_ml(df: pd.DataFrame, periodo: str = '12m',
                     peso_score: float = None, peso_saldo: float = None,
                     peso_estagnacao: float = None) -> pd.DataFrame:
    """
    Calcula score de Machine Learning para priorização.

    Args:
        df: DataFrame
        periodo: '12m' ou '60m'
        peso_score: Peso do score de risco (padrão: 0.4)
        peso_saldo: Peso do saldo (padrão: 0.3)
        peso_estagnacao: Peso da estagnação (padrão: 0.3)

    Returns:
        DataFrame com scores ML
    """
    df_ml = df.copy()

    # Usar pesos padrão se não fornecidos
    if peso_score is None:
        peso_score = ML_PARAMS['peso_score']
    if peso_saldo is None:
        peso_saldo = ML_PARAMS['peso_saldo']
    if peso_estagnacao is None:
        peso_estagnacao = ML_PARAMS['peso_estagnacao']

    col_score = get_col_name('score_risco', periodo)

    # Normalizar componentes (0-100)
    max_score = df_ml[col_score].max() if col_score in df_ml.columns and df_ml[col_score].max() > 0 else 1
    max_saldo = df_ml['saldo_credor_atual'].max() if df_ml['saldo_credor_atual'].max() > 0 else 1

    df_ml['score_norm'] = (df_ml[col_score] / max_score * 100) if col_score in df_ml.columns else 0
    df_ml['saldo_norm'] = (df_ml['saldo_credor_atual'] / max_saldo * 100)
    df_ml['estagnacao_norm'] = (df_ml['qtde_ultimos_12m_iguais'] / 13 * 100)

    # Calcular score ML
    df_ml['score_ml'] = (
        df_ml['score_norm'] * peso_score +
        df_ml['saldo_norm'] * peso_saldo +
        df_ml['estagnacao_norm'] * peso_estagnacao
    )

    # Classificar em níveis de alerta
    df_ml['nivel_alerta_ml'] = pd.cut(
        df_ml['score_ml'],
        bins=ML_PARAMS['bins_alerta'],
        labels=ML_PARAMS['labels_alerta']
    )

    # Prioridade numérica (1 = mais urgente)
    df_ml['prioridade_ml'] = df_ml['score_ml'].rank(method='dense', ascending=False)

    return df_ml


def calcular_indicadores_fraude(df: pd.DataFrame, periodo: str = '12m') -> pd.DataFrame:
    """
    Calcula indicadores avançados de fraude.

    Args:
        df: DataFrame
        periodo: '12m' ou '60m'

    Returns:
        DataFrame com indicadores
    """
    df_fraude = df.copy()

    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)
    col_desvio = get_col_name('desvio_padrao_credito', periodo)
    col_media = get_col_name('media_credito', periodo)

    # Indicador 1: Crescimento anormal
    if col_cresc in df_fraude.columns:
        df_fraude['ind_crescimento_anormal'] = (
            df_fraude[col_cresc] > THRESHOLDS['crescimento_anormal']
        ).astype(int)

    # Indicador 2: Saldo alto + estagnação
    df_fraude['ind_alto_estagnado'] = (
        (df_fraude['saldo_credor_atual'] > THRESHOLDS['saldo_medio']) &
        (df_fraude['qtde_ultimos_12m_iguais'] >= THRESHOLDS['meses_estagnado_min'])
    ).astype(int)

    # Indicador 3: Baixa variação + alto saldo
    if col_desvio in df_fraude.columns and col_media in df_fraude.columns:
        df_fraude['ind_baixa_variacao'] = (
            (df_fraude[col_desvio] < THRESHOLDS['desvio_padrao_baixo']) &
            (df_fraude[col_media] > THRESHOLDS['saldo_medio'])
        ).astype(int)

    # Indicador 4: Saldo extremamente alto
    df_fraude['ind_saldo_extremo'] = (
        df_fraude['saldo_credor_atual'] > THRESHOLDS['saldo_alto']
    ).astype(int)

    # Score consolidado de fraude
    cols_indicadores = [col for col in df_fraude.columns if col.startswith('ind_')]
    if cols_indicadores:
        df_fraude['score_fraude_calculado'] = df_fraude[cols_indicadores].sum(axis=1)

    return df_fraude


def calcular_metricas_comparativas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas comparativas entre 12m e 60m.

    Args:
        df: DataFrame com dados de ambos períodos

    Returns:
        DataFrame com métricas comparativas
    """
    df_comp = df.copy()

    # Score
    if 'score_risco_12m' in df_comp.columns and 'score_risco_60m' in df_comp.columns:
        df_comp['variacao_score'] = df_comp['score_risco_12m'] - df_comp['score_risco_60m']
        df_comp['variacao_score_pct'] = safe_divide(
            df_comp['variacao_score'],
            df_comp['score_risco_60m'],
            0
        ) * 100

    # Saldo
    if 'saldo_12m_atras' in df_comp.columns and 'saldo_60m_atras' in df_comp.columns:
        df_comp['variacao_saldo_periodo'] = (
            df_comp['saldo_credor_atual'] - df_comp['saldo_12m_atras']
        )
        df_comp['variacao_saldo_total'] = (
            df_comp['saldo_credor_atual'] - df_comp['saldo_60m_atras']
        )

    # Crescimento
    if 'crescimento_saldo_percentual_12m' in df_comp.columns and 'crescimento_saldo_percentual_60m' in df_comp.columns:
        df_comp['aceleracao_crescimento'] = (
            df_comp['crescimento_saldo_percentual_12m'] -
            df_comp['crescimento_saldo_percentual_60m']
        )

    # Classificação de mudança
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
    """
    Calcula concentração de risco por agrupamento.

    Args:
        df: DataFrame
        group_col: Coluna para agrupar

    Returns:
        Dicionário com métricas de concentração
    """
    if df.empty or group_col not in df.columns:
        return {}

    # Saldo por grupo
    saldo_grupo = df.groupby(group_col)['saldo_credor_atual'].sum().sort_values(ascending=False)

    # Top 5 e Top 10
    top5_saldo = saldo_grupo.head(5).sum()
    top10_saldo = saldo_grupo.head(10).sum()
    total_saldo = saldo_grupo.sum()

    # Índice de Gini (concentração)
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


def calcular_tendencias(df: pd.DataFrame, periodo: str = '12m') -> Dict:
    """
    Calcula tendências gerais dos dados.

    Args:
        df: DataFrame
        periodo: '12m' ou '60m'

    Returns:
        Dicionário com tendências
    """
    if df.empty:
        return {}

    col_cresc = get_col_name('crescimento_saldo_percentual', periodo)

    tendencias = {}

    # Tendência de saldo
    if col_cresc in df.columns:
        cresc_medio = df[col_cresc].mean()
        if cresc_medio > 50:
            tendencias['saldo'] = 'CRESCIMENTO FORTE'
        elif cresc_medio > 10:
            tendencias['saldo'] = 'CRESCIMENTO MODERADO'
        elif cresc_medio > -10:
            tendencias['saldo'] = 'ESTÁVEL'
        else:
            tendencias['saldo'] = 'DECRESCENTE'

    # Tendência de risco
    col_score = get_col_name('score_risco', periodo)
    if col_score in df.columns:
        score_medio = df[col_score].mean()
        if score_medio > 70:
            tendencias['risco'] = 'MUITO ALTO'
        elif score_medio > 50:
            tendencias['risco'] = 'ALTO'
        elif score_medio > 30:
            tendencias['risco'] = 'MODERADO'
        else:
            tendencias['risco'] = 'BAIXO'

    # Tendência de estagnação
    pct_estagnados = (df['qtde_ultimos_12m_iguais'] >= 6).sum() / len(df) * 100
    if pct_estagnados > 50:
        tendencias['estagnacao'] = 'CRÍTICA'
    elif pct_estagnados > 25:
        tendencias['estagnacao'] = 'ALTA'
    elif pct_estagnados > 10:
        tendencias['estagnacao'] = 'MODERADA'
    else:
        tendencias['estagnacao'] = 'BAIXA'

    return tendencias
