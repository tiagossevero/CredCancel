# 📊 Gerador Automático de Data Schemas

## 🎯 Objetivo

Este script automatiza a geração de documentação completa de schemas para todas as tabelas do projeto CRED-CANCEL v3.0.

## 📦 O que é gerado?

Para cada uma das **29 tabelas** (16 originais + 13 intermediárias), o script gera automaticamente:

1. **`*_describe.txt`** - Schema detalhado com `DESCRIBE FORMATTED`
2. **`*_select.txt`** - Amostra de 10 registros com `SELECT * LIMIT 10`
3. **`*_metadata.txt`** - Metadados (total de registros, colunas, data de extração)

## 🚀 Como executar

### Opção 1: Jupyter Notebook (RECOMENDADO)

```bash
# No ambiente com acesso ao banco
jupyter notebook GERAR_DATA_SCHEMAS.ipynb
```

Execute as células sequencialmente. O notebook está dividido em seções:
- ✅ Configuração e imports
- 🎯 **Prioridade ALTA** - Tabelas do Streamlit (executar primeiro)
- 📁 Tabelas Originais
- 🔄 Tabelas Intermediárias
- 📊 Relatório Final

### Opção 2: Script Python

```bash
python gerar_data_schemas.py
```

## 📋 Tabelas Processadas

### 🎯 PRIORIDADE ALTA (Streamlit Dashboard)
- `teste.credito_dime_completo` ⭐
- `teste.credito_dime_textil` ⭐
- `teste.credito_dime_metalmec` ⭐
- `teste.credito_dime_tech` ⭐

### 📁 TABELAS ORIGINAIS (16 tabelas)

#### usr_sat_ods (6 tabelas)
- `ods_decl_dime_raw`
- `vw_cad_contrib`
- `vw_ods_pagamento`
- `vw_sna_pgdasd_grupo_empresarial`
- `vw_ods_contrib`
- `vw_ods_dcip`

#### usr_sat_cadastro (3 tabelas)
- `ruc_protocolo`
- `ruc_general`
- `tab_sit_cad`

#### usr_sat_shared (2 tabelas)
- `tab_generica`
- `tab_munic`

#### usr_sat_auditoria (2 tabelas)
- `aud_empresa_sob_suspeita`
- `aud_empresa_suspeita`

### 🔄 TABELAS INTERMEDIÁRIAS (13 tabelas - banco `teste`)

#### Análise de Crédito (5 tabelas)
- `credito_dime`
- `credito_dime_completo`
- `credito_dime_textil`
- `credito_dime_metalmec`
- `credito_dime_tech`

#### Análise de Cancelamento (8 tabelas)
- `cancel_cnpj`
- `cancel_cadastro`
- `cancel_recolhimento`
- `cancel_suspeitas`
- `cancel_suspeitas_score`
- `cancel_zero_normal`
- `cancel_zero_simples`
- `cancel_final`

## 📂 Estrutura de Saída

```
data_schemas/
├── INDEX.txt                      # Índice completo de todas as tabelas
├── originais/                     # Tabelas usr_sat_*
│   ├── usr_sat_ods.ods_decl_dime_raw_describe.txt
│   ├── usr_sat_ods.ods_decl_dime_raw_select.txt
│   ├── usr_sat_ods.ods_decl_dime_raw_metadata.txt
│   ├── usr_sat_ods.vw_cad_contrib_describe.txt
│   ├── usr_sat_ods.vw_cad_contrib_select.txt
│   ├── usr_sat_ods.vw_cad_contrib_metadata.txt
│   └── ... (outros arquivos)
└── intermediarias/                # Tabelas teste.*
    ├── teste.credito_dime_completo_describe.txt
    ├── teste.credito_dime_completo_select.txt
    ├── teste.credito_dime_completo_metadata.txt
    ├── teste.credito_dime_textil_describe.txt
    ├── teste.credito_dime_textil_select.txt
    ├── teste.credito_dime_textil_metadata.txt
    └── ... (outros arquivos)
```

## 📄 Exemplo de Arquivos Gerados

### 1. Arquivo `*_describe.txt`
```
================================================================================
DESCRIBE FORMATTED
================================================================================

col_name                        data_type            comment
nu_cnpj                         bigint               Número do CNPJ
saldo_credor_atual              decimal(15,2)        Saldo credor atual
score_risco_12m                 decimal(5,2)         Score de risco 12 meses
...
```

### 2. Arquivo `*_select.txt`
```
================================================================================
SELECT * FROM teste.credito_dime_completo LIMIT 10
================================================================================

nu_cnpj | saldo_credor_atual | score_risco_12m | classificacao_risco_12m
------------------------------------------------------------------------
12345678901234 | 150000.00 | 85.50 | CRÍTICO
98765432109876 | 25000.00 | 45.20 | MÉDIO
...

Total de registros exibidos: 10
```

### 3. Arquivo `*_metadata.txt`
```
================================================================================
METADADOS
================================================================================
Tabela: teste.credito_dime_completo
Total de registros: 125,348
Total de colunas: 87
Data da extração: 2025-11-17 14:30:22

Colunas:
  - nu_cnpj
  - saldo_credor_atual
  - score_risco_12m
  ...
```

## ⚙️ Requisitos

### Python/PySpark
```python
from pyspark.sql import SparkSession
```

### Ambiente
- Acesso ao banco Impala SEF/SC
- Credenciais configuradas
- Ambiente com PySpark disponível

## 🔧 Personalização

### Adicionar mais tabelas

Edite o arquivo e adicione à lista correspondente:

```python
TABELAS_INTERMEDIARIAS = {
    'teste': [
        'credito_dime_completo',
        'sua_nova_tabela',  # ← adicionar aqui
        # ...
    ]
}
```

### Mudar quantidade de registros de amostra

Altere o LIMIT na query:

```python
df_select = spark.sql(f"SELECT * FROM {tabela_completa} LIMIT 20")  # ← alterar aqui
```

### Mudar diretório de saída

```python
OUTPUT_DIR = "meu_diretorio_customizado"  # ← alterar aqui
```

## 🐛 Troubleshooting

### Erro: "Table not found"
- Verifique se você tem acesso à tabela
- Confirme se o nome da tabela está correto
- Verifique se o database está correto

### Erro: "Permission denied"
- Verifique suas credenciais
- Confirme se você tem permissão de leitura nas tabelas

### Spark Session não encontrada
No notebook, use:
```python
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
```

## 📊 Output Esperado

Ao final da execução, você verá:

```
================================================================================
📊 RELATÓRIO FINAL - GERAÇÃO DE DATA SCHEMAS
================================================================================

📅 Data/Hora: 2025-11-17 14:30:45

📈 Total de tabelas processadas: 29
   ✅ Sucesso: 29
   ❌ Falhas: 0

💾 Total de registros nas tabelas: 15,234,567

📁 Arquivos salvos em: /home/user/CredCancel/data_schemas/
```

## 🎯 Próximos Passos

Após gerar os schemas:

1. ✅ Revisar o arquivo `INDEX.txt` para visão geral
2. ✅ Usar arquivos `*_describe.txt` para documentação de API/schemas
3. ✅ Usar arquivos `*_select.txt` para exemplos de dados
4. ✅ Criar documentação markdown a partir dos schemas
5. ✅ Adicionar ao repositório Git

## 📝 Notas

- O script **não modifica** nenhuma tabela, apenas lê dados
- É seguro executar quantas vezes forem necessárias
- Os arquivos são sobrescritos a cada execução
- Tempo estimado de execução: 5-15 minutos (depende do tamanho das tabelas)

## 🆘 Suporte

Em caso de dúvidas ou problemas:
1. Verifique os arquivos `*_ERRO.txt` no diretório de saída
2. Revise os logs do console/notebook
3. Confirme acesso ao banco de dados

---

**Desenvolvido para**: CRED-CANCEL v3.0
**Data**: Novembro 2025
**Versão**: 1.0
