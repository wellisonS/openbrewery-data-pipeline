# OpenBrewery Data Pipeline

## 📌 Sobre o projeto

Este projeto implementa um **pipeline de dados end-to-end** para ingestão, transformação e modelagem analítica de dados da [Open Brewery DB API](https://www.openbrewerydb.org/), seguindo a arquitetura **Medallion (Bronze → Silver → Gold)**.

A orquestração é feita com **Apache Airflow** containerizado via **Docker Compose**, as transformações rodam no **Databricks** via PySpark, e a camada Gold é modelada com **dbt**.

---

## 🏗️ Arquitetura

```
AIRFLOW (Docker)
      ↓ orquestra
      ├─→ Bronze: ingestão da API → Databricks (bronze.openbrewery_raw)
      ├─→ Silver: transformação PySpark → Databricks (silver.openbrewery)
      └─→ Gold: modelagem dbt → Databricks (gold.gold_openbrewery)
```

### Camadas

- 🟤 **Bronze** — dados brutos da API em formato Delta, sem transformações
- 🟠 **Silver** — dados limpos, tipados, sem duplicatas, particionados por `country` e `state`
- 🟢 **Gold** — view agregada com contagem de cervejarias por tipo e localização, pronta para consumo analítico

---

## 🧱 Estrutura do projeto

```text
openbrewery-data-pipeline/
├── airflow/
│   └── dags/
│       └── openbrewery_pipeline.py       # DAG principal
├── databricks/
│   ├── bronze/
│   │   ├── bronze_job.ipynb              # Notebook de entrada
│   │   └── ingest_openbrewery_bronze.py  # Lógica de ingestão
│   └── silver/
│       ├── silver_job.ipynb              # Notebook de entrada
│       └── transform_openbrewery_silver.py # Lógica de transformação
├── dbt/
│   └── openbrewery/
│       ├── models/
│       │   └── example/
│       │       └── gold/
│       │           ├── gold_openbrewery.sql
│       │           └── gold_openbrewery.yml
│       ├── sources/
│       │   └── silver.yml
│       └── dbt_project.yml
├── tests/
│   ├── test_bronze.py                    # Testes unitários da camada bronze
│   └── test_silver.py                    # Testes unitários da camada silver
├── docker-compose.yml
├── Dockerfile
└── .env                                  # Variáveis de ambiente (não versionado)
```

---

## 🛠️ Pré-requisitos

- [Docker](https://www.docker.com/) & Docker Compose instalados
- Conta e workspace no [Databricks](https://databricks.com/)
- Token de autenticação do Databricks
- Python 3.10+

---

## 🚀 Como Rodar

### 1. Clonar o repositório

```bash
git clone https://github.com/seu-usuario/openbrewery-data-pipeline.git
cd openbrewery-data-pipeline
```

### 2. Configurar variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
DATABRICKS_HOST=seu-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/seu-warehouse-id
DATABRICKS_TOKEN=seu-token-aqui
```

### 3. Subir o Airflow

```bash
docker compose up -d --build
```

Acesse `http://localhost:8080` com as credenciais:
- **Usuário:** `admin`
- **Senha:** `admin`

### 4. Configurar a Connection do Databricks no Airflow

1. Vá em **Admin → Connections**
2. Edite ou crie a connection `databricks_default`:
   - **Conn Type:** Databricks
   - **Host:** `https://seu-workspace.cloud.databricks.com`
   - **Extra:** `{"token": "seu-token-aqui"}`

### 5. Criar os Jobs no Databricks

No workspace do Databricks, crie dois jobs apontando para os notebooks:
- **Bronze:** `databricks/bronze/bronze_job`
- **Silver:** `databricks/silver/silver_job`

Atualize os `job_id` no arquivo `airflow/dags/openbrewery_pipeline.py`.

### 6. Disparar o pipeline

Na UI do Airflow, ative a DAG `openbrewery_pipeline` e clique em **▶ Trigger DAG**.

---

## 🧪 Testes

Os testes unitários cobrem as funções de ingestão e transformação das camadas Bronze e Silver.

### Instalar dependências

```bash
pip install pytest pytest-mock pyspark
```

### Rodar os testes

```bash
pytest tests/ -v
```

---

## 📐 Decisões de Design

| Decisão | Justificativa |
|---|---|
| Airflow em Docker | Facilita reprodutibilidade e portabilidade do ambiente |
| Databricks para Bronze e Silver | PySpark nativo para processamento distribuído e armazenamento Delta |
| dbt para Gold | Separação clara entre transformação de dados e modelagem analítica |
| Particionamento por `country` e `state` | Melhora performance em queries filtradas por localização |
| Modo `overwrite` nas escritas | Garante idempotência — reexecutar o pipeline não gera duplicatas |
| Variáveis de ambiente para credenciais | Nenhuma credencial é versionada no repositório |

---

## 📊 Monitoramento e Alertas

### Monitoramento do Pipeline

**Airflow** oferece monitoramento nativo da DAG:
- Cada task tem status visual (success, failed, running) na UI
- O histórico de execuções fica disponível em **Browse → DAG Runs**
- Retries automáticos podem ser configurados por task:

```python
bronze = DatabricksRunNowOperator(
    task_id="bronze_ingestion",
    retries=3,
    retry_delay=timedelta(minutes=5),
    ...
)
```

### Alertas por E-mail em caso de falha

O Airflow suporta alertas automáticos por e-mail configurando `email_on_failure`:

```python
default_args = {
    "email": ["seu-email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}
```

Em produção, isso seria integrado com um servidor SMTP ou serviço como SendGrid.

### Qualidade de Dados

A qualidade dos dados é validada em duas camadas:

**dbt tests na camada Gold:**
- `not_null` nos campos críticos (`state`, `brewery_type`, `breweries_count`)
- `accepted_values` para garantir que `brewery_type` contém apenas valores válidos da API

**Testes unitários no código Python:**
- Validação de schema e tipos de dados
- Verificação de remoção de duplicatas e nulos

### Em produção, o monitoramento seria complementado com:

- **Databricks Job Alerts** — notificações nativas por e-mail ou webhook quando um job falha
- **Logs centralizados** — integração com ferramentas como Datadog ou CloudWatch para agregação de logs
- **Data Quality com Great Expectations** — validações mais granulares como verificar volume mínimo de registros, distribuição de valores, e freshness dos dados
- **SLA Misses no Airflow** — alertas quando uma DAG não termina dentro do tempo esperado

---

## 🔄 Trade-offs

- **SQLite como metastore do Airflow** — adequado para desenvolvimento local, mas em produção deve ser substituído por PostgreSQL para suportar execuções paralelas e persistência de dados entre reinicializações.
- **Serverless compute no Databricks** — simplifica a configuração, mas em produção seria avaliado um cluster dedicado para workloads previsíveis e maior controle de custo.
- **dbt rodando no container do Airflow** — funciona para este projeto, mas em escala seria mais adequado usar o **dbt Cloud** ou um container dedicado para isolamento de dependências.