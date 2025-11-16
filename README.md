# CRED-CANCEL v2.0 💰

Sistema de análise e detecção de fraudes em créditos acumulados de ICMS desenvolvido para a **Receita Estadual de Santa Catarina (SEF/SC)**.

## 📋 Sobre o Projeto

O **CRED-CANCEL** é uma ferramenta estratégica de inteligência fiscal que utiliza big data e machine learning para identificar padrões fraudulentos em créditos acumulados de ICMS (Imposto sobre Circulação de Mercadorias e Serviços). O sistema auxilia auditores fiscais na priorização de ações fiscais e na identificação de contribuintes com comportamentos suspeitos relacionados a créditos tributários.

### Objetivos Principais

- 🔍 **Detecção de Fraudes**: Identificar padrões fraudulentos em créditos acumulados de ICMS
- 📊 **Análise de Padrões**: Monitorar comportamentos suspeitos ao longo de períodos de 12 e 60 meses
- 🎯 **Priorização Inteligente**: Utilizar machine learning para ranquear empresas por risco
- ⚠️ **Alertas Automáticos**: Sistema de alertas para empresas com indicadores críticos
- 📈 **Suporte à Decisão**: Auxiliar na decisão de cancelamento de inscrições estaduais
- 💼 **Análise Setorial**: Monitoramento específico por setor econômico (têxtil, metalmecânica, tecnologia)

## ✨ Funcionalidades

### Dashboards Analíticos

O sistema oferece 15 painéis especializados:

1. **Dashboard Executivo** - Visão geral com principais KPIs e métricas
2. **Comparativo 12m vs 60m** - Análise comparativa entre períodos recentes e históricos
3. **Análise de Suspeitas** - Detecção automática de empresas com comportamento fraudulento
4. **Ranking de Empresas** - Classificação de contribuintes por nível de risco
5. **Análise Setorial** - Análises específicas por setor econômico
6. **Drill-Down Empresa** - Investigação detalhada de empresas individuais
7. **Machine Learning** - Sistema de pontuação e priorização baseado em ML
8. **Padrões de Abuso** - Identificação de padrões de uso abusivo de créditos
9. **Empresas Inativas** - Monitoramento de empresas inativas com créditos pendentes
10. **Reforma Tributária** - Projeção de impactos da reforma tributária
11. **Empresas com Noteiras** - Análise de relacionamento com empresas "noteiras"
12. **Declarações Zeradas** - Identificação de padrões de declarações zeradas
13. **Alertas Automáticos** - Sistema de notificações de risco
14. **Guia de Cancelamento IE** - Orientações para cancelamento de inscrições
15. **Sobre o Sistema** - Informações técnicas e metodológicas

### Indicadores de Risco

O sistema monitora múltiplos indicadores de fraude:

- ⏸️ **Estagnação de Créditos**: Valores repetidos ao longo do tempo
- 📈 **Crescimento Anômalo**: Variações percentuais e absolutas suspeitas
- 💰 **Acúmulo Excessivo**: Saldos credores desproporcionais
- 🔄 **Crédito Presumido**: Padrões de utilização de crédito presumido
- ❌ **Status de Cancelamento**: Empresas canceladas com créditos pendentes
- 0️⃣ **Declarações Zeradas**: Sequências suspeitas de declarações sem movimento
- 🏢 **Relacionamento com Noteiras**: Vínculos com empresas de fachada

### Pontuação de Machine Learning

Sistema de scoring baseado em três pilares:

```
Score ML = (Score de Risco Normalizado × 0.4) +
           (Saldo Normalizado × 0.3) +
           (Score de Estagnação × 0.3)
```

**Níveis de Alerta:**
- 🟢 **BAIXO** (0-20): Risco mínimo
- 🟡 **MÉDIO** (20-40): Monitoramento recomendado
- 🟠 **ALTO** (40-60): Atenção necessária
- 🔴 **CRÍTICO** (60-80): Prioridade para fiscalização
- 🆘 **EMERGENCIAL** (80-100): Ação imediata requerida

## 🛠️ Tecnologias Utilizadas

### Core
- **Python 3.x** - Linguagem principal
- **Streamlit** - Framework web para dashboards interativos
- **Apache Impala** - Engine de consultas SQL para big data

### Bibliotecas Python
- **pandas** - Manipulação e análise de dados
- **numpy** - Computação numérica
- **plotly** - Visualizações interativas
- **SQLAlchemy** - Conectividade com banco de dados
- **hashlib** - Criptografia e autenticação

### Infraestrutura de Dados
- **Impala Database** - Armazenamento e processamento de dados fiscais
- **JSON** - Formato de queries e metadados
- **Jupyter Notebooks** - Análises exploratórias

## 📦 Pré-requisitos

### Sistema
- Python 3.7 ou superior
- Acesso à rede interna da SEF/SC
- Credenciais LDAP para acesso ao Impala

### Bibliotecas Python
```bash
pip install streamlit
pip install pandas
pip install numpy
pip install plotly
pip install sqlalchemy
pip install impyla
```

## 🚀 Instalação

1. **Clone o repositório**
```bash
git clone https://github.com/tiagossevero/CredCancel.git
cd CredCancel
```

2. **Instale as dependências**
```bash
pip install -r requirements.txt
```

3. **Configure as credenciais**

Crie o arquivo `.streamlit/secrets.toml` com as credenciais do Impala:
```toml
[impala]
user = "seu_usuario_ldap"
password = "sua_senha_ldap"
```

## ⚙️ Configuração

### Conexão com o Banco de Dados

O sistema se conecta ao servidor Impala da SEF/SC:
- **Host**: `bdaworkernode02.sef.sc.gov.br`
- **Porta**: `21050`
- **Database**: `teste`
- **Autenticação**: LDAP com SSL

### Tabelas Utilizadas

- `credito_dime_completo` - Dataset completo de créditos ICMS
- `credito_dime_textil` - Dados do setor têxtil
- `credito_dime_metalmec` - Dados do setor metalmecânico
- `credito_dime_tech` - Dados do setor de tecnologia

## 💻 Uso

### Iniciar o Sistema

```bash
streamlit run CRED.py
```

O sistema estará disponível em: `http://localhost:8501`

### Autenticação

1. Acesse a aplicação no navegador
2. Digite a senha de acesso
3. Navegue pelos painéis disponíveis no menu lateral

### Fluxo de Trabalho Recomendado

1. **Dashboard Executivo** - Visão geral da situação atual
2. **Análise de Suspeitas** - Identificar empresas com alto risco
3. **Machine Learning** - Priorizar por score automático
4. **Drill-Down Empresa** - Investigar empresas específicas
5. **Guia de Cancelamento** - Orientações para procedimentos

## 📁 Estrutura do Projeto

```
CredCancel/
│
├── CRED.py                      # Aplicação principal Streamlit (3.200 linhas)
│
├── CANCEL.json                  # Query de dados de cancelamento (169KB)
├── CRED-CANCEL.json            # Dicionário de dados principal (272KB)
├── CREDITO DIME EFD.json       # Query de dados EFD (420KB)
│
├── CREDITO.ipynb               # Notebook principal de análise (2.6MB)
├── CREDITO-Exemplo.ipynb       # Notebook de exemplos (144KB)
│
└── README.md                    # Documentação do projeto
```

## 📊 Arquitetura do Sistema

### Camadas da Aplicação

```
┌─────────────────────────────────────┐
│   Interface Web (Streamlit)         │
│   - 15 Painéis Analíticos           │
│   - Visualizações Interativas       │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│   Camada de Processamento           │
│   - Cálculo de Scores de Risco      │
│   - Machine Learning                │
│   - Normalização de Dados           │
└─────────────────┬───────────────────┘
                  │
┌─────────────────▼───────────────────┐
│   Camada de Dados (Apache Impala)   │
│   - Big Data Warehouse              │
│   - Queries SQL Otimizadas          │
│   - Cache de 1 hora (TTL)           │
└─────────────────────────────────────┘
```

### Pipeline de Análise

1. **Extração**: Dados carregados do Impala via SQLAlchemy
2. **Transformação**: Normalização, limpeza e cálculo de KPIs
3. **Análise**: Aplicação de algoritmos de detecção de padrões
4. **Scoring**: Machine Learning para priorização
5. **Visualização**: Dashboards interativos com Plotly
6. **Ação**: Geração de alertas e recomendações

## 🔒 Segurança

- 🔐 **Autenticação**: Sistema de senha para acesso ao dashboard
- 🔑 **LDAP**: Autenticação corporativa para acesso ao banco de dados
- 🔒 **SSL**: Conexões criptografadas com o Impala
- 💾 **Secrets Management**: Credenciais gerenciadas via Streamlit Secrets
- 🏛️ **Uso Interno**: Sistema restrito à rede da SEF/SC

> ⚠️ **Atenção**: Este sistema contém dados fiscais confidenciais. Uso restrito a servidores autorizados da Receita Estadual de Santa Catarina.

## 📈 Estatísticas do Projeto

- **Linhas de Código**: ~3.200 linhas Python
- **Funções**: 28+ funções especializadas
- **Painéis**: 15 dashboards analíticos
- **Indicadores**: 7+ indicadores de risco
- **Setores Monitorados**: 3 setores econômicos
- **Período de Análise**: 12 e 60 meses

## 🔄 Metodologia de Análise

### Análise Dual-Period

O sistema compara dois períodos críticos:
- **12 meses**: Comportamento recente e tendências atuais
- **60 meses**: Histórico de longo prazo para contexto

### Cálculo de Risco Base

```python
Risk Score = Σ (indicadores_fraude) +
             peso_estagnacao +
             peso_crescimento_anomalo +
             peso_relacionamento_noteiras
```

### Normalização para ML

Todos os scores são normalizados para escala 0-100 para facilitar:
- Comparação entre empresas
- Definição de thresholds
- Priorização automática

## 🎯 Casos de Uso

### 1. Identificação de Fraudes Estruturadas
Empresas que mantêm créditos estagnados por longos períodos sem justificativa econômica.

### 2. Planejamento Tributário Abusivo
Identificação de contribuintes que utilizam créditos presumidos de forma irregular.

### 3. Empresas Inativas
Monitoramento de empresas canceladas ou inativas que mantêm créditos acumulados.

### 4. Relacionamento com Noteiras
Detecção de vínculos com empresas de fachada para fraudes documentais.

### 5. Análise Setorial
Comparação de comportamento entre empresas do mesmo setor para identificar outliers.

## 🤝 Contribuindo

Este é um projeto interno da Receita Estadual de Santa Catarina. Contribuições são restritas a servidores autorizados.

Para sugestões ou melhorias, contate o desenvolvedor:
- **Tiago Severo** - AFRE (Auditor Fiscal da Receita Estadual)

## 👨‍💻 Autor

**Tiago Severo**
- Cargo: AFRE - Auditor Fiscal da Receita Estadual
- Instituição: Secretaria de Estado da Fazenda de Santa Catarina (SEF/SC)
- GitHub: [@tiagossevero](https://github.com/tiagossevero)

## 📄 Licença

Este projeto é de propriedade do **Governo do Estado de Santa Catarina** e é restrito ao uso interno da Receita Estadual. Todos os direitos reservados.

**Uso não autorizado, reprodução ou distribuição deste sistema é estritamente proibido.**

---

## 📞 Suporte

Para dúvidas, problemas técnicos ou solicitações de acesso, entre em contato com:
- **Área de TI - SEF/SC**
- **Coordenação de Inteligência Fiscal**

---

## 🔖 Versão

**v2.0** - Sistema CRED-CANCEL
- Data de Atualização: 2025
- Status: Em Produção
- Ambiente: Rede Interna SEF/SC

---

<div align="center">

**Desenvolvido com 💙 para a Receita Estadual de Santa Catarina**

*Combatendo fraudes fiscais com inteligência de dados*

</div>
