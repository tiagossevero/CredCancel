"""
Funções utilitárias do sistema CRED-CANCEL v3.0
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union
from .config import COLUNAS_PERIODO


def get_col_name(base_name: str, periodo: str = '12m') -> str:
    """
    Retorna o nome correto da coluna baseado no período.

    Args:
        base_name: Nome base da coluna
        periodo: '12m', '60m' ou 'comparativo'

    Returns:
        Nome correto da coluna
    """
    if base_name in COLUNAS_PERIODO:
        col_template = COLUNAS_PERIODO[base_name]
        if '{periodo}' in col_template:
            return col_template.format(periodo=periodo)
        return col_template

    return base_name


def formatar_valor(valor: Union[int, float], tipo: str = 'numero') -> str:
    """
    Formata valores para exibição.

    Args:
        valor: Valor a formatar
        tipo: 'numero', 'moeda', 'percentual', 'decimal'

    Returns:
        String formatada
    """
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

        else:  # numero
            return f'{int(valor):,}'

    except:
        return str(valor)


def calcular_percentil(df: pd.DataFrame, coluna: str, valor: float) -> float:
    """
    Calcula em qual percentil um valor se encontra.

    Args:
        df: DataFrame
        coluna: Nome da coluna
        valor: Valor a verificar

    Returns:
        Percentil (0-100)
    """
    if df.empty or coluna not in df.columns:
        return 0.0

    try:
        return (df[coluna] <= valor).sum() / len(df) * 100
    except:
        return 0.0


def classificar_valor(valor: float, thresholds: Dict[str, float]) -> str:
    """
    Classifica um valor baseado em thresholds.

    Args:
        valor: Valor a classificar
        thresholds: Dicionário com limites (ex: {'baixo': 10, 'medio': 50, 'alto': 100})

    Returns:
        Classificação
    """
    sorted_thresholds = sorted(thresholds.items(), key=lambda x: x[1])

    for label, limit in sorted_thresholds:
        if valor <= limit:
            return label.upper()

    return list(thresholds.keys())[-1].upper()


def criar_faixas(valores: pd.Series, num_faixas: int = 5,
                 labels: Optional[List[str]] = None) -> pd.Series:
    """
    Cria faixas (bins) para uma série de valores.

    Args:
        valores: Série com valores
        num_faixas: Número de faixas
        labels: Labels customizadas (opcional)

    Returns:
        Série com faixas
    """
    try:
        if labels and len(labels) != num_faixas:
            raise ValueError("Número de labels deve ser igual ao número de faixas")

        return pd.cut(valores, bins=num_faixas, labels=labels)
    except:
        return valores


def calcular_variacao_percentual(valor_atual: float, valor_anterior: float) -> float:
    """
    Calcula variação percentual entre dois valores.

    Args:
        valor_atual: Valor atual
        valor_anterior: Valor anterior

    Returns:
        Variação percentual
    """
    if valor_anterior == 0:
        return 0.0 if valor_atual == 0 else float('inf')

    return ((valor_atual - valor_anterior) / abs(valor_anterior)) * 100


def remover_outliers(df: pd.DataFrame, coluna: str,
                     metodo: str = 'iqr', fator: float = 1.5) -> pd.DataFrame:
    """
    Remove outliers de um DataFrame.

    Args:
        df: DataFrame
        coluna: Coluna para análise
        metodo: 'iqr' ou 'zscore'
        fator: Fator multiplicador (padrão: 1.5 para IQR)

    Returns:
        DataFrame sem outliers
    """
    if df.empty or coluna not in df.columns:
        return df

    if metodo == 'iqr':
        Q1 = df[coluna].quantile(0.25)
        Q3 = df[coluna].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - fator * IQR
        upper = Q3 + fator * IQR
        return df[(df[coluna] >= lower) & (df[coluna] <= upper)]

    elif metodo == 'zscore':
        from scipy import stats
        z_scores = np.abs(stats.zscore(df[coluna].dropna()))
        return df[(z_scores < fator)]

    return df


def agrupar_dados(df: pd.DataFrame, grupo_col: str,
                  agg_dict: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
    """
    Agrupa dados de forma simplificada.

    Args:
        df: DataFrame
        grupo_col: Coluna para agrupar
        agg_dict: Dicionário de agregações

    Returns:
        DataFrame agrupado
    """
    if df.empty or grupo_col not in df.columns:
        return pd.DataFrame()

    try:
        return df.groupby(grupo_col).agg(agg_dict).reset_index()
    except Exception as e:
        return pd.DataFrame()


def normalizar_valores(serie: pd.Series, metodo: str = 'minmax') -> pd.Series:
    """
    Normaliza uma série de valores.

    Args:
        serie: Série a normalizar
        metodo: 'minmax' (0-1) ou 'zscore' (média 0, desvio 1)

    Returns:
        Série normalizada
    """
    if serie.empty:
        return serie

    if metodo == 'minmax':
        min_val = serie.min()
        max_val = serie.max()
        if max_val == min_val:
            return pd.Series([0.5] * len(serie), index=serie.index)
        return (serie - min_val) / (max_val - min_val)

    elif metodo == 'zscore':
        return (serie - serie.mean()) / serie.std()

    return serie


def calcular_estatisticas_descritivas(serie: pd.Series) -> Dict:
    """
    Calcula estatísticas descritivas completas.

    Args:
        serie: Série numérica

    Returns:
        Dicionário com estatísticas
    """
    if serie.empty:
        return {}

    return {
        'count': len(serie),
        'mean': serie.mean(),
        'median': serie.median(),
        'std': serie.std(),
        'min': serie.min(),
        'max': serie.max(),
        'q25': serie.quantile(0.25),
        'q75': serie.quantile(0.75),
        'iqr': serie.quantile(0.75) - serie.quantile(0.25),
        'cv': (serie.std() / serie.mean() * 100) if serie.mean() != 0 else 0,
        'missing': serie.isnull().sum(),
        'missing_pct': (serie.isnull().sum() / len(serie)) * 100
    }


def criar_ranking(df: pd.DataFrame, coluna_score: str,
                  top_n: Optional[int] = None) -> pd.DataFrame:
    """
    Cria ranking baseado em uma coluna.

    Args:
        df: DataFrame
        coluna_score: Coluna para ranquear
        top_n: Número de registros no topo (opcional)

    Returns:
        DataFrame ranqueado
    """
    if df.empty or coluna_score not in df.columns:
        return pd.DataFrame()

    df_rank = df.copy()
    df_rank['ranking'] = df_rank[coluna_score].rank(method='dense', ascending=False)
    df_rank = df_rank.sort_values('ranking')

    if top_n:
        df_rank = df_rank.head(top_n)

    return df_rank


def detectar_padroes_temporais(serie_temporal: pd.Series,
                              janela: int = 3) -> Dict:
    """
    Detecta padrões em série temporal.

    Args:
        serie_temporal: Série ordenada temporalmente
        janela: Tamanho da janela para médias móveis

    Returns:
        Dicionário com padrões detectados
    """
    if serie_temporal.empty or len(serie_temporal) < janela:
        return {}

    # Médias móveis
    ma = serie_temporal.rolling(window=janela).mean()

    # Tendência
    if serie_temporal.iloc[-1] > serie_temporal.iloc[0]:
        tendencia = 'CRESCENTE'
    elif serie_temporal.iloc[-1] < serie_temporal.iloc[0]:
        tendencia = 'DECRESCENTE'
    else:
        tendencia = 'ESTÁVEL'

    # Volatilidade
    volatilidade = serie_temporal.std() / serie_temporal.mean() if serie_temporal.mean() != 0 else 0

    return {
        'tendencia': tendencia,
        'volatilidade': volatilidade,
        'media_movel': ma.iloc[-1] if not ma.empty else 0,
        'crescimento_total': calcular_variacao_percentual(
            serie_temporal.iloc[-1],
            serie_temporal.iloc[0]
        )
    }


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida formato de CNPJ.

    Args:
        cnpj: String com CNPJ

    Returns:
        True se válido
    """
    # Remove caracteres não numéricos
    cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))

    # CNPJ deve ter 14 dígitos
    if len(cnpj_limpo) != 14:
        return False

    # Validação básica (não verifica dígitos verificadores)
    return True


def formatar_cnpj(cnpj: Union[str, int]) -> str:
    """
    Formata CNPJ para exibição.

    Args:
        cnpj: CNPJ (string ou int)

    Returns:
        CNPJ formatado (00.000.000/0000-00)
    """
    cnpj_str = str(cnpj).zfill(14)

    if len(cnpj_str) != 14:
        return cnpj_str

    return f'{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:14]}'


def safe_divide(numerador: float, denominador: float,
                default: float = 0.0) -> float:
    """
    Divisão segura (evita divisão por zero).

    Args:
        numerador: Numerador
        denominador: Denominador
        default: Valor padrão se divisão inválida

    Returns:
        Resultado da divisão ou valor padrão
    """
    try:
        if denominador == 0:
            return default
        return numerador / denominador
    except:
        return default


def consolidar_duplicatas(df: pd.DataFrame, chave: str,
                         metodo: str = 'first') -> pd.DataFrame:
    """
    Consolida registros duplicados.

    Args:
        df: DataFrame
        chave: Coluna chave para identificar duplicatas
        metodo: 'first', 'last', 'sum', 'mean'

    Returns:
        DataFrame sem duplicatas
    """
    if df.empty or chave not in df.columns:
        return df

    if metodo in ['first', 'last']:
        return df.drop_duplicates(subset=[chave], keep=metodo)

    elif metodo in ['sum', 'mean']:
        return df.groupby(chave).agg(metodo).reset_index()

    return df
