# 📄 CRED-CANCEL v3.0 - Versão Monolítica

## 🎯 Sobre esta Versão

Este é o arquivo **monolítico** do sistema CRED-CANCEL v3.0, consolidando todas as funcionalidades em um único arquivo Python (`app_monolitico.py`) para facilitar a implantação em servidores que requerem execução de arquivo único.

## 📦 Conteúdo do Arquivo Monolítico

O arquivo `app_monolitico.py` (2.225 linhas, 79KB) consolida todos os módulos do projeto:

- **Configurações** - Todas as constantes e parâmetros do sistema
- **Funções Utilitárias** - Formatação, validação, cálculos gerais
- **Autenticação** - Sistema de login e segurança
- **Banco de Dados** - Conexão com Impala e carregamento de dados
- **Filtros** - Sistema completo de filtros dinâmicos
- **Métricas** - Cálculos de KPIs, scores e análises
- **Visualizações** - Gráficos Plotly interativos
- **Exportação** - Exportação para Excel e CSV
- **Aplicação Principal** - Dashboard Streamlit com 12 páginas de análise

## 🚀 Como Executar

### Pré-requisitos

```bash
pip install streamlit pandas numpy plotly sqlalchemy impyla xlsxwriter
```

### Execução

```bash
streamlit run app_monolitico.py
```

Ou especificando porta:

```bash
streamlit run app_monolitico.py --server.port 8501
```

## 🔑 Autenticação

O sistema requer senha de acesso. A senha padrão está configurada no arquivo.

Para produção, recomenda-se:
1. Alterar a senha diretamente no código (linha 182): `SENHA_ACESSO = "sua_senha"`
2. Ou usar secrets do Streamlit: criar `.streamlit/secrets.toml`

## 🗄️ Configuração do Banco de Dados

O arquivo está configurado para conectar ao Impala. Configure as credenciais em:

**.streamlit/secrets.toml**
```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

Ou edite diretamente as configurações no arquivo (linhas 186-193).

## 📊 Funcionalidades Incluídas

### Páginas de Análise

1. **🏠 Dashboard Executivo** - KPIs principais e visão geral
2. **📊 Análise Comparativa** - Comparação 12m vs 60m
3. **🔍 Análise de Suspeitas** - Empresas suspeitas detectadas
4. **🏆 Ranking de Empresas** - Top empresas por diferentes critérios
5. **🏭 Análise Setorial** - Análise por setor (Têxtil, Metal-Mecânico, Tech)
6. **🔬 Drill-Down** - Análise detalhada por CNPJ
7. **🤖 Machine Learning** - Priorização inteligente de casos
8. **⚠️ Padrões de Abuso** - Detecção de fraudes
9. **💤 Empresas Inativas** - Empresas sem movimentação
10. **📋 Empresas Noteiras** - Detecção de notas frias
11. **0️⃣ Declarações Zeradas** - Análise de omissões
12. **ℹ️ Sobre o Sistema** - Informações e documentação

### Recursos

- ✅ **Filtros Dinâmicos** - Contexto, período, risco, GERFE, fraude
- ✅ **Machine Learning** - Score de priorização com IA
- ✅ **Exportação** - Excel, CSV e relatórios completos
- ✅ **Visualizações** - Gráficos interativos Plotly
- ✅ **Cache Inteligente** - Performance otimizada
- ✅ **Interface Responsiva** - Design moderno e intuitivo

## 🔧 Vantagens da Versão Monolítica

### ✅ Prós

- **Simplicidade de Deploy** - Um único arquivo para copiar
- **Sem Dependências de Módulos** - Não requer estrutura de pastas
- **Fácil Distribuição** - Envie apenas um arquivo
- **Ideal para Servidores Simples** - Funciona em qualquer ambiente Python

### ⚠️ Contras

- **Manutenção** - Mais difícil de manter código em arquivo único
- **Colaboração** - Menos adequado para múltiplos desenvolvedores
- **Modularidade** - Perde a organização em módulos separados

## 📌 Quando Usar Cada Versão

### Use a Versão Monolítica quando:
- ❌ Precisa rodar em servidor com restrições de estrutura de arquivos
- ❌ Quer distribuir como arquivo único
- ❌ Ambiente não suporta importações de módulos locais
- ❌ Deploy simplificado é prioridade

### Use a Versão Modular quando:
- ✅ Está desenvolvendo e mantendo o código
- ✅ Trabalha em equipe
- ✅ Precisa de organização e separação de responsabilidades
- ✅ Quer facilitar testes unitários

## 🔄 Diferenças em Relação à Versão Modular

A versão monolítica é **funcionalmente idêntica** à versão modular, mas:

1. **Estrutura de Arquivos**
   - **Modular:** `app.py` + pasta `modules/` com 8 arquivos
   - **Monolítico:** Apenas `app_monolitico.py`

2. **Imports**
   - **Modular:** `from modules.config import ...`
   - **Monolítico:** Tudo no mesmo arquivo, sem imports relativos

3. **Organização**
   - **Modular:** Separado por responsabilidade (config, auth, database, etc.)
   - **Monolítico:** Tudo sequencial em um único arquivo

## 📝 Estrutura do Código Monolítico

```
app_monolitico.py (2.225 linhas)
├── Importações (linhas 1-30)
├── Configurações (linhas 31-450)
├── Funções Utilitárias (linhas 451-650)
├── Autenticação (linhas 651-750)
├── Banco de Dados (linhas 751-950)
├── Filtros (linhas 951-1150)
├── Métricas (linhas 1151-1450)
├── Visualizações (linhas 1451-1650)
├── Exportação (linhas 1651-1800)
├── Aplicação Principal (linhas 1801-2220)
└── Execução (linhas 2221-2225)
```

## 🐛 Troubleshooting

### Erro: "ModuleNotFoundError"
Instale as dependências: `pip install -r requirements.txt`

### Erro: "Connection refused" (Impala)
Verifique as credenciais e acesso ao servidor Impala

### Erro: "Authentication failed"
Atualize a senha no arquivo ou em `.streamlit/secrets.toml`

### Performance lenta
- Verifique cache do Streamlit
- Ajuste `CACHE_CONFIG` no arquivo (linha 239)

## 📞 Suporte

**Desenvolvedor:** AFRE Tiago Severo
**Órgão:** SEF/SC - Receita Estadual de Santa Catarina
**Versão:** 3.0.0 (Monolítico)
**Data:** Novembro 2025

## 📄 Licença

© 2025 SEF/SC - Secretaria da Fazenda de Santa Catarina
Todos os direitos reservados.

---

**Nota:** Este arquivo foi gerado automaticamente a partir da versão modular do projeto.
Para desenvolvimento, prefira usar a versão modular em `app.py` + `modules/`.
