"""
Script para Gerar Data Schemas Automaticamente
==============================================
Executa DESCRIBE FORMATTED e SELECT * LIMIT 10 para todas as tabelas do projeto CRED-CANCEL

Uso:
    python gerar_data_schemas.py

Requisitos:
    - Acesso ao ambiente PySpark
    - Credenciais configuradas
"""

from pyspark.sql import SparkSession
import os
from datetime import datetime


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

# Diretório de saída
OUTPUT_DIR = "data_schemas"

# Definição de todas as tabelas do projeto
TABELAS_ORIGINAIS = {
    'usr_sat_ods': [
        'ods_decl_dime_raw',
        'vw_cad_contrib',
        'vw_ods_pagamento',
        'vw_sna_pgdasd_grupo_empresarial',
        'vw_ods_contrib',
        'vw_ods_dcip'
    ],
    'usr_sat_cadastro': [
        'ruc_protocolo',
        'ruc_general',
        'tab_sit_cad'
    ],
    'usr_sat_shared': [
        'tab_generica',
        'tab_munic'
    ],
    'usr_sat_auditoria': [
        'aud_empresa_sob_suspeita',
        'aud_empresa_suspeita'
    ]
}

TABELAS_INTERMEDIARIAS = {
    'teste': [
        'credito_dime',
        'credito_dime_completo',
        'credito_dime_textil',
        'credito_dime_metalmec',
        'credito_dime_tech',
        'cancel_cnpj',
        'cancel_cadastro',
        'cancel_recolhimento',
        'cancel_suspeitas',
        'cancel_suspeitas_score',
        'cancel_zero_normal',
        'cancel_zero_simples',
        'cancel_final'
    ]
}


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def criar_diretorios():
    """Cria estrutura de diretórios para os schemas"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/originais", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/intermediarias", exist_ok=True)
    print(f"✅ Diretórios criados em: {OUTPUT_DIR}/")


def salvar_resultado(conteudo, tipo, database, tabela):
    """Salva resultado em arquivo"""
    categoria = "originais" if database != "teste" else "intermediarias"
    filepath = f"{OUTPUT_DIR}/{categoria}/{database}.{tabela}_{tipo}.txt"

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(conteudo)

    return filepath


def formatar_describe(df):
    """Formata output do DESCRIBE FORMATTED"""
    linhas = []
    linhas.append("=" * 80)
    linhas.append("DESCRIBE FORMATTED")
    linhas.append("=" * 80)
    linhas.append("")

    for row in df.collect():
        col_name = row.col_name if hasattr(row, 'col_name') else row[0]
        data_type = row.data_type if hasattr(row, 'data_type') else row[1]
        comment = row.comment if hasattr(row, 'comment') else (row[2] if len(row) > 2 else '')

        linhas.append(f"{col_name:<30} {data_type:<20} {comment}")

    return "\n".join(linhas)


def formatar_select(df, tabela_nome):
    """Formata output do SELECT * LIMIT 10"""
    linhas = []
    linhas.append("=" * 80)
    linhas.append(f"SELECT * FROM {tabela_nome} LIMIT 10")
    linhas.append("=" * 80)
    linhas.append("")

    # Cabeçalho
    colunas = df.columns
    linhas.append(" | ".join(colunas))
    linhas.append("-" * 80)

    # Dados
    rows = df.collect()
    if len(rows) == 0:
        linhas.append("(Nenhum registro encontrado)")
    else:
        for row in rows:
            valores = [str(val) if val is not None else "NULL" for val in row]
            linhas.append(" | ".join(valores))

    linhas.append("")
    linhas.append(f"Total de registros exibidos: {len(rows)}")

    return "\n".join(linhas)


def processar_tabela(spark, database, tabela):
    """Processa uma tabela individual"""
    tabela_completa = f"{database}.{tabela}"

    print(f"\n📊 Processando: {tabela_completa}")
    print("-" * 60)

    try:
        # 1. DESCRIBE FORMATTED
        print("  🔍 Executando DESCRIBE FORMATTED...")
        df_describe = spark.sql(f"DESCRIBE FORMATTED {tabela_completa}")
        conteudo_describe = formatar_describe(df_describe)
        arquivo_describe = salvar_resultado(conteudo_describe, "describe", database, tabela)
        print(f"  ✅ DESCRIBE salvo: {arquivo_describe}")

        # 2. SELECT * LIMIT 10
        print("  🔍 Executando SELECT * LIMIT 10...")
        df_select = spark.sql(f"SELECT * FROM {tabela_completa} LIMIT 10")
        conteudo_select = formatar_select(df_select, tabela_completa)
        arquivo_select = salvar_resultado(conteudo_select, "select", database, tabela)
        print(f"  ✅ SELECT salvo: {arquivo_select}")

        # 3. Metadados adicionais
        print("  📈 Coletando metadados...")
        count = spark.sql(f"SELECT COUNT(*) as total FROM {tabela_completa}").collect()[0].total

        metadata = []
        metadata.append("=" * 80)
        metadata.append("METADADOS")
        metadata.append("=" * 80)
        metadata.append(f"Tabela: {tabela_completa}")
        metadata.append(f"Total de registros: {count:,}")
        metadata.append(f"Total de colunas: {len(df_select.columns)}")
        metadata.append(f"Data da extração: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        arquivo_metadata = salvar_resultado("\n".join(metadata), "metadata", database, tabela)
        print(f"  ✅ METADATA salvo: {arquivo_metadata}")

        print(f"  ✅ Concluído: {tabela_completa} ({count:,} registros)")

        return True

    except Exception as e:
        print(f"  ❌ ERRO em {tabela_completa}: {str(e)}")

        # Salvar log de erro
        erro_msg = f"ERRO ao processar {tabela_completa}\n"
        erro_msg += f"Timestamp: {datetime.now()}\n"
        erro_msg += f"Erro: {str(e)}\n"
        salvar_resultado(erro_msg, "ERRO", database, tabela)

        return False


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal"""
    print("=" * 80)
    print("🚀 GERADOR DE DATA SCHEMAS - CRED-CANCEL v3.0")
    print("=" * 80)
    print()

    # Criar diretórios
    criar_diretorios()

    # Inicializar Spark
    print("⚡ Inicializando SparkSession...")
    spark = SparkSession.builder \
        .appName("DataSchema Generator") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"✅ Spark {spark.version} inicializado com sucesso!")
    print()

    # Estatísticas
    total_tabelas = 0
    sucesso = 0
    falhas = 0

    # Processar tabelas ORIGINAIS
    print("\n" + "=" * 80)
    print("📁 PROCESSANDO TABELAS ORIGINAIS")
    print("=" * 80)

    for database, tabelas in TABELAS_ORIGINAIS.items():
        print(f"\n🗂️  Database: {database}")
        for tabela in tabelas:
            total_tabelas += 1
            if processar_tabela(spark, database, tabela):
                sucesso += 1
            else:
                falhas += 1

    # Processar tabelas INTERMEDIÁRIAS
    print("\n" + "=" * 80)
    print("🔄 PROCESSANDO TABELAS INTERMEDIÁRIAS")
    print("=" * 80)

    for database, tabelas in TABELAS_INTERMEDIARIAS.items():
        print(f"\n🗂️  Database: {database}")
        for tabela in tabelas:
            total_tabelas += 1
            if processar_tabela(spark, database, tabela):
                sucesso += 1
            else:
                falhas += 1

    # Relatório final
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"Total de tabelas processadas: {total_tabelas}")
    print(f"✅ Sucesso: {sucesso}")
    print(f"❌ Falhas: {falhas}")
    print(f"📁 Arquivos salvos em: {os.path.abspath(OUTPUT_DIR)}/")
    print()

    # Gerar índice
    gerar_indice(total_tabelas, sucesso, falhas)

    # Fechar Spark
    spark.stop()
    print("\n✅ Processo concluído!")


def gerar_indice(total, sucesso, falhas):
    """Gera arquivo índice com resumo"""
    indice = []
    indice.append("=" * 80)
    indice.append("ÍNDICE DE DATA SCHEMAS - CRED-CANCEL v3.0")
    indice.append("=" * 80)
    indice.append(f"Data de geração: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    indice.append(f"Total de tabelas: {total}")
    indice.append(f"Processadas com sucesso: {sucesso}")
    indice.append(f"Falhas: {falhas}")
    indice.append("")
    indice.append("=" * 80)
    indice.append("TABELAS ORIGINAIS")
    indice.append("=" * 80)

    for database, tabelas in TABELAS_ORIGINAIS.items():
        indice.append(f"\n📁 {database}:")
        for tabela in tabelas:
            indice.append(f"  - {database}.{tabela}")

    indice.append("")
    indice.append("=" * 80)
    indice.append("TABELAS INTERMEDIÁRIAS")
    indice.append("=" * 80)

    for database, tabelas in TABELAS_INTERMEDIARIAS.items():
        indice.append(f"\n📁 {database}:")
        for tabela in tabelas:
            indice.append(f"  - {database}.{tabela}")

    indice.append("")
    indice.append("=" * 80)
    indice.append("ESTRUTURA DE ARQUIVOS")
    indice.append("=" * 80)
    indice.append("")
    indice.append("Para cada tabela, foram gerados 3 arquivos:")
    indice.append("  1. <database>.<tabela>_describe.txt  - Schema detalhado")
    indice.append("  2. <database>.<tabela>_select.txt    - Amostra de dados")
    indice.append("  3. <database>.<tabela>_metadata.txt  - Metadados")
    indice.append("")

    filepath = f"{OUTPUT_DIR}/INDEX.txt"
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("\n".join(indice))

    print(f"📋 Índice gerado: {filepath}")


# =============================================================================
# EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    main()
