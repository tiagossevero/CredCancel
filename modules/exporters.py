"""
Módulo de exportação de dados do sistema CRED-CANCEL v3.0
"""

import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import datetime
from typing import Optional, Dict


def exportar_para_excel(df: pd.DataFrame, nome_arquivo: Optional[str] = None) -> BytesIO:
    """
    Exporta DataFrame para Excel.

    Args:
        df: DataFrame a exportar
        nome_arquivo: Nome do arquivo (opcional)

    Returns:
        BytesIO com conteúdo Excel
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Dados', index=False)

        # Formatação
        workbook = writer.book
        worksheet = writer.sheets['Dados']

        # Formato de cabeçalho
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#1565c0',
            'font_color': 'white',
            'border': 1
        })

        # Aplicar formato ao cabeçalho
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, col_num, 15)

    output.seek(0)
    return output


def exportar_para_csv(df: pd.DataFrame) -> BytesIO:
    """
    Exporta DataFrame para CSV.

    Args:
        df: DataFrame a exportar

    Returns:
        BytesIO com conteúdo CSV
    """
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8-sig', sep=';')
    output.seek(0)
    return output


def exportar_multiplas_abas(dados: Dict[str, pd.DataFrame],
                            nome_arquivo: Optional[str] = None) -> BytesIO:
    """
    Exporta múltiplos DataFrames para Excel com abas separadas.

    Args:
        dados: Dicionário com nome_aba: DataFrame
        nome_arquivo: Nome do arquivo (opcional)

    Returns:
        BytesIO com conteúdo Excel
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for nome_aba, df in dados.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=nome_aba[:31], index=False)  # Max 31 chars

    output.seek(0)
    return output


def criar_relatorio_completo(df: pd.DataFrame, kpis: Dict,
                             titulo: str = "Relatório CRED-CANCEL") -> BytesIO:
    """
    Cria relatório completo com dados e KPIs.

    Args:
        df: DataFrame com dados
        kpis: Dicionário com KPIs
        titulo: Título do relatório

    Returns:
        BytesIO com Excel
    """
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Aba de resumo
        df_resumo = pd.DataFrame([kpis]).T
        df_resumo.columns = ['Valor']
        df_resumo.to_excel(writer, sheet_name='Resumo')

        # Aba de dados
        df.to_excel(writer, sheet_name='Dados Completos', index=False)

        # Formatação
        workbook = writer.book

        # Criar formato de título
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'fg_color': '#1565c0',
            'font_color': 'white'
        })

        # Adicionar título na aba de resumo
        worksheet_resumo = writer.sheets['Resumo']
        worksheet_resumo.write(0, 0, titulo, title_format)

    output.seek(0)
    return output


def criar_botao_download_excel(df: pd.DataFrame, label: str = "📥 Baixar Excel",
                               nome_arquivo: Optional[str] = None):
    """
    Cria botão de download para Excel.

    Args:
        df: DataFrame a exportar
        label: Label do botão
        nome_arquivo: Nome do arquivo
    """
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
    """
    Cria botão de download para CSV.

    Args:
        df: DataFrame a exportar
        label: Label do botão
        nome_arquivo: Nome do arquivo
    """
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
    """
    Cria painel completo de exportação.

    Args:
        df: DataFrame principal
        kpis: KPIs (opcional)
        dados_extras: DataFrames extras para exportação (opcional)
    """
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

    # Se houver dados extras, criar exportação com múltiplas abas
    if dados_extras:
        st.divider()
        st.write("**Exportação Avançada:**")

        # Combinar dados principais com extras
        todos_dados = {'Principal': df}
        todos_dados.update(dados_extras)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_multiplo = f"cred_cancel_completo_{timestamp}.xlsx"
        excel_multiplo = exportar_multiplas_abas(todos_dados)

        st.download_button(
            label="📚 Excel com Múltiplas Abas",
            data=excel_multiplo,
            file_name=nome_multiplo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
