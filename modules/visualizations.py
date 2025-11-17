"""
Módulo de visualizações do sistema CRED-CANCEL v3.0
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Optional, List, Dict, Union
from .config import CLASSIFICACOES_RISCO


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


def criar_grafico_linha(df: pd.DataFrame, x: str, y: Union[str, List[str]],
                       titulo: str, tema: str = 'plotly_white') -> go.Figure:
    """Cria gráfico de linhas."""
    if df.empty:
        return go.Figure()

    if isinstance(y, str):
        y = [y]

    fig = go.Figure()

    for col in y:
        fig.add_trace(go.Scatter(
            x=df[x],
            y=df[col],
            mode='lines+markers',
            name=col
        ))

    fig.update_layout(
        title=titulo,
        template=tema,
        hovermode='x unified'
    )

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


def criar_histograma(df: pd.DataFrame, coluna: str, titulo: str,
                    tema: str = 'plotly_white', nbins: int = 30) -> go.Figure:
    """Cria histograma."""
    if df.empty or coluna not in df.columns:
        return go.Figure()

    fig = px.histogram(
        df,
        x=coluna,
        title=titulo,
        template=tema,
        nbins=nbins
    )

    return fig


def criar_boxplot(df: pd.DataFrame, y: str, x: Optional[str] = None,
                 titulo: str = '', tema: str = 'plotly_white') -> go.Figure:
    """Cria boxplot."""
    if df.empty or y not in df.columns:
        return go.Figure()

    fig = px.box(
        df,
        x=x,
        y=y,
        title=titulo,
        template=tema
    )

    return fig


def criar_treemap(df: pd.DataFrame, path: List[str], values: str,
                 titulo: str, tema: str = 'plotly_white') -> go.Figure:
    """Cria treemap."""
    if df.empty:
        return go.Figure()

    fig = px.treemap(
        df,
        path=path,
        values=values,
        title=titulo,
        template=tema
    )

    return fig


def criar_funil(df: pd.DataFrame, x: str, y: str, titulo: str,
               tema: str = 'plotly_white') -> go.Figure:
    """Cria gráfico de funil."""
    if df.empty:
        return go.Figure()

    fig = go.Figure(go.Funnel(
        y=df[y],
        x=df[x],
        textinfo="value+percent initial"
    ))

    fig.update_layout(
        title=titulo,
        template=tema
    )

    return fig


def criar_heatmap(df: pd.DataFrame, titulo: str,
                 tema: str = 'plotly_white') -> go.Figure:
    """Cria heatmap de correlação."""
    if df.empty:
        return go.Figure()

    # Selecionar apenas colunas numéricas
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


def criar_sunburst(df: pd.DataFrame, path: List[str], values: str,
                  titulo: str, tema: str = 'plotly_white') -> go.Figure:
    """Cria gráfico sunburst."""
    if df.empty:
        return go.Figure()

    fig = px.sunburst(
        df,
        path=path,
        values=values,
        title=titulo,
        template=tema
    )

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


# Mapa de cores padrão
COLOR_MAP_RISCO = {
    'CRÍTICO': CLASSIFICACOES_RISCO['CRÍTICO']['cor'],
    'ALTO': CLASSIFICACOES_RISCO['ALTO']['cor'],
    'MÉDIO': CLASSIFICACOES_RISCO['MÉDIO']['cor'],
    'BAIXO': CLASSIFICACOES_RISCO['BAIXO']['cor']
}
