"""
Script para Gerar Data Schema de UMA ÚNICA TABELA
=================================================
Útil para testes e debugging

Uso:
    python gerar_schema_single.py <database> <tabela>

Exemplo:
    python gerar_schema_single.py teste credito_dime_completo
"""

from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime


def main():
    # Validar argumentos
    if len(sys.argv) != 3:
        print("Uso: python gerar_schema_single.py <database> <tabela>")
        print()
        print("Exemplos:")
        print("  python gerar_schema_single.py teste credito_dime_completo")
        print("  python gerar_schema_single.py usr_sat_ods ods_decl_dime_raw")
        sys.exit(1)

    database = sys.argv[1]
    tabela = sys.argv[2]
    tabela_completa = f"{database}.{tabela}"

    print("=" * 80)
    print("🔍 GERADOR DE SCHEMA - TABELA ÚNICA")
    print("=" * 80)
    print(f"📊 Tabela: {tabela_completa}")
    print()

    # Inicializar Spark
    print("⚡ Inicializando Spark...")
    spark = SparkSession.builder \
        .appName("Single Table Schema") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"✅ Spark {spark.version} inicializado")
    print()

    try:
        # 1. DESCRIBE FORMATTED
        print("🔍 Executando DESCRIBE FORMATTED...")
        print("-" * 80)
        df_describe = spark.sql(f"DESCRIBE FORMATTED {tabela_completa}")
        df_describe.show(100, truncate=False)
        print()

        # 2. Metadados
        print("📈 Coletando metadados...")
        print("-" * 80)
        count = spark.sql(f"SELECT COUNT(*) as total FROM {tabela_completa}").collect()[0].total
        print(f"Total de registros: {count:,}")
        print()

        # 3. SELECT * LIMIT 10
        print("🔍 Executando SELECT * LIMIT 10...")
        print("-" * 80)
        df_select = spark.sql(f"SELECT * FROM {tabela_completa} LIMIT 10")
        df_select.show(10, truncate=50)
        print()

        # 4. Schema do DataFrame
        print("📋 Schema do DataFrame:")
        print("-" * 80)
        df_select.printSchema()
        print()

        # 5. Estatísticas descritivas (para colunas numéricas)
        print("📊 Estatísticas descritivas (colunas numéricas):")
        print("-" * 80)
        df_select.describe().show()
        print()

        # Resumo final
        print("=" * 80)
        print("✅ PROCESSAMENTO CONCLUÍDO")
        print("=" * 80)
        print(f"Tabela: {tabela_completa}")
        print(f"Registros: {count:,}")
        print(f"Colunas: {len(df_select.columns)}")
        print()
        print("Colunas encontradas:")
        for i, col in enumerate(df_select.columns, 1):
            print(f"  {i:2d}. {col}")

    except Exception as e:
        print("=" * 80)
        print("❌ ERRO")
        print("=" * 80)
        print(f"Erro ao processar {tabela_completa}")
        print(f"Detalhes: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
