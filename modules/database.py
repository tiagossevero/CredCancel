"""
Módulo de conexão e carregamento de dados do Impala
"""

import streamlit as st
import pandas as pd
import ssl
from sqlalchemy import create_engine
from typing import Dict, Optional
from .config import IMPALA_CONFIG, TABELAS, CACHE_CONFIG


# Configuração SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


@st.cache_resource
def get_impala_engine():
    """
    Cria e retorna engine de conexão com Impala.

    Returns:
        Engine do SQLAlchemy ou None em caso de erro
    """
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

        # Testar conexão
        with engine.connect() as conn:
            pass

        return engine

    except Exception as e:
        st.sidebar.error(f"❌ Erro na conexão: {str(e)[:150]}")
        return None


@st.cache_data(ttl=CACHE_CONFIG['ttl_dados'])
def carregar_dados_creditos(_engine) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados de todas as tabelas DIME.

    Args:
        _engine: Engine de conexão Impala

    Returns:
        Dicionário com DataFrames de cada tabela
    """
    dados = {}

    if _engine is None:
        st.sidebar.error("❌ Engine de conexão inválida")
        return {}

    # Testar conexão
    try:
        with _engine.connect() as conn:
            st.sidebar.success("✅ Conexão Impala OK!")
    except Exception as e:
        st.sidebar.error(f"❌ Falha na conexão: {str(e)[:150]}")
        return {}

    # Carregar tabelas
    st.sidebar.write("📊 **Carregando dados:**")
    st.sidebar.write("")

    for key, table_name in TABELAS.items():
        try:
            with st.sidebar.spinner(f"Carregando {table_name}..."):
                # Query
                query = f"SELECT * FROM {IMPALA_CONFIG['database']}.{table_name}"

                # Carregar dados
                df = pd.read_sql(query, _engine)

                # Normalizar nomes de colunas
                df.columns = [col.lower() for col in df.columns]

                # Converter colunas numéricas
                for col in df.select_dtypes(include=['object']).columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass

                dados[key] = df

                # Feedback detalhado
                if key in ['textil', 'metalmec', 'tech']:
                    flag_col = f'flag_setor_{key}'
                    if flag_col in df.columns:
                        qtd_flag = (df[flag_col] == 1).sum()
                        st.sidebar.write(
                            f"  ✓ **{table_name}:** {len(df):,} registros "
                            f"({qtd_flag:,} ativos)"
                        )
                    else:
                        st.sidebar.write(f"  ✓ **{table_name}:** {len(df):,} registros")
                else:
                    st.sidebar.write(f"  ✓ **{table_name}:** {len(df):,} registros")

        except Exception as e:
            st.sidebar.error(f"  ❌ Erro em {table_name}")
            st.sidebar.caption(f"  {str(e)[:100]}")
            dados[key] = pd.DataFrame()

    # Resumo final
    st.sidebar.write("")
    st.sidebar.write("---")
    st.sidebar.write("📋 **Resumo Final:**")

    total_registros = sum(len(df) for df in dados.values())
    tabelas_ok = sum(1 for df in dados.values() if not df.empty)

    st.sidebar.write(f"  ✓ Tabelas carregadas: {tabelas_ok}/{len(TABELAS)}")
    st.sidebar.write(f"  ✓ Total de registros: {total_registros:,}")

    if total_registros == 0:
        st.sidebar.error("⚠️ Nenhum dado foi carregado!")

    return dados


def get_table_info(df: pd.DataFrame, nome_tabela: str) -> Dict:
    """
    Retorna informações sobre uma tabela.

    Args:
        df: DataFrame da tabela
        nome_tabela: Nome da tabela

    Returns:
        Dicionário com informações
    """
    if df.empty:
        return {
            'nome': nome_tabela,
            'registros': 0,
            'colunas': 0,
            'memoria_mb': 0,
            'tipos': {}
        }

    return {
        'nome': nome_tabela,
        'registros': len(df),
        'colunas': len(df.columns),
        'memoria_mb': df.memory_usage(deep=True).sum() / 1024**2,
        'tipos': df.dtypes.value_counts().to_dict(),
        'colunas_lista': df.columns.tolist(),
        'nulos': df.isnull().sum().sum()
    }


def verificar_qualidade_dados(dados: Dict[str, pd.DataFrame]) -> Dict:
    """
    Verifica a qualidade dos dados carregados.

    Args:
        dados: Dicionário com DataFrames

    Returns:
        Dicionário com métricas de qualidade
    """
    qualidade = {}

    for key, df in dados.items():
        if df.empty:
            qualidade[key] = {
                'status': 'VAZIO',
                'score': 0,
                'problemas': ['Tabela vazia']
            }
            continue

        problemas = []

        # Verificar valores nulos
        pct_nulos = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        if pct_nulos > 50:
            problemas.append(f'Muitos nulos ({pct_nulos:.1f}%)')

        # Verificar duplicatas
        if df.duplicated().sum() > 0:
            pct_dup = (df.duplicated().sum() / len(df)) * 100
            if pct_dup > 5:
                problemas.append(f'Duplicatas ({pct_dup:.1f}%)')

        # Verificar colunas esperadas
        colunas_essenciais = ['nu_cnpj', 'saldo_credor_atual']
        faltando = [col for col in colunas_essenciais if col not in df.columns]
        if faltando:
            problemas.append(f'Colunas faltando: {", ".join(faltando)}')

        # Calcular score
        if not problemas:
            score = 100
            status = 'EXCELENTE'
        elif len(problemas) == 1:
            score = 70
            status = 'BOM'
        elif len(problemas) == 2:
            score = 40
            status = 'REGULAR'
        else:
            score = 20
            status = 'RUIM'

        qualidade[key] = {
            'status': status,
            'score': score,
            'problemas': problemas,
            'pct_nulos': pct_nulos
        }

    return qualidade


def get_connection_status() -> Dict:
    """
    Verifica o status da conexão.

    Returns:
        Dicionário com informações de status
    """
    engine = get_impala_engine()

    if engine is None:
        return {
            'conectado': False,
            'mensagem': 'Falha ao criar engine',
            'detalhes': None
        }

    try:
        with engine.connect() as conn:
            return {
                'conectado': True,
                'mensagem': 'Conexão ativa',
                'host': IMPALA_CONFIG['host'],
                'porta': IMPALA_CONFIG['port'],
                'database': IMPALA_CONFIG['database'],
                'usuario': IMPALA_CONFIG['user']
            }
    except Exception as e:
        return {
            'conectado': False,
            'mensagem': 'Erro ao conectar',
            'erro': str(e)
        }


@st.cache_data(ttl=CACHE_CONFIG['ttl_dados'])
def executar_query_customizada(_engine, query: str) -> Optional[pd.DataFrame]:
    """
    Executa uma query SQL customizada.

    Args:
        _engine: Engine de conexão
        query: Query SQL

    Returns:
        DataFrame com resultados ou None
    """
    if _engine is None:
        return None

    try:
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao executar query: {str(e)}")
        return None
