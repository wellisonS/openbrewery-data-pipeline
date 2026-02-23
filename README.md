# OpenBrewery Data Pipeline

## 📌 Sobre o projeto

Este projeto implementa um **pipeline de dados usando dbt** para processar dados da Open Brewery API seguindo uma arquitetura em camadas (**Bronze → Silver → Gold**).

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
│   ├── Dockerfile
├── databricks/
│   ├── bronze_layer/
│   │   ├── bronze_job.ipynb
│   │   └── ingest_openbrewery_bronze.py
│   ├── silver_layer/
│   │   ├── silver_job.ipynb
│   │   └── transform_openbrewery_silver.py
├── dbt/
│   └── openbrewery/
│       ├── models/
│       │   └── gold/
│       └── dbt_project.yml
├── docker-compose.yml
```
---

## ⚙️ Requisitos

### Software
- Python 3.9+
- dbt Core 1.11+
- Databricks CLI (ou acesso configurado ao Databricks)
- Git

### Principais dependências Python

dbt-core==1.11.2
dbt-databricks==1.11.4
