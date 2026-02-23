# OpenBrewery Data Pipeline

## 📌 Sobre o projeto

Este projeto implementa um **pipeline de dados utilizando dbt** para processar dados da Open Brewery API seguindo uma arquitetura em camadas (**Bronze → Silver → Gold**).

O objetivo é:
- Transformar dados brutos em **dados analíticos confiáveis**
- Aplicar boas práticas de **modelagem, testes e documentação**
- Disponibilizar uma camada **Gold pronta para consumo analítico**

## 📌 Visão Geral

Este projeto implementa um **pipeline de dados** para ingestão, processamento e modelagem de dados da API OpenBrewery.  
O fluxo segue uma arquitetura em camadas:

- 🟤 **Bronze** – ingestão de dados brutos da API
- 🟠 **Silver** – transformação intermediária (limpeza / padrão)
- 🟢 **Gold** – modelagem analítica via dbt

A orquestração é feita com **Apache Airflow** containerizado via **Docker Compose**.

---

## 🧱 Estrutura do projeto

```text
openbrewery-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── openbrewery_pipeline.py
├── databricks/
│   ├── bronze_layer/
│   │   ├── bronze_job.ipynb
│   │   └── ingest_openbrewery_bronze.py
│   ├── silver_layer/
│   │   ├── silver_job.ipynb
│   │   └── transform_openbrewery_silver.py
│   ├── gold_layer/
│   │   ├── tests.ipynb
├── dbt/
│   └── openbrewery/
│       ├── models/
│       │   └── gold/
│       └── dbt_project.yml
├── docker-compose.yml
├── Dockerfile
```
---

## ⚙️ Requisitos

---

## 🛠️ Pré-requisitos

- Docker & Docker Compose instalados
- Conta e workspace no Databricks
- Token de autenticação do Databricks
- Python (para editar notebooks / scripts)

---

## 🚀 Como Rodar

### 1. Ajustar Configurações

No Airflow UI:

1. Acesse: `http://localhost:8080`
2. Configure a **Connection** para o Databricks:
   - Host:
   - Token:
   - HTTP Path:

Configure também
- Variáveis de ambiente necessárias
- Paths para dados, se aplicável

---

### 2. Subir a Orquestração

```bash
docker compose up --build

