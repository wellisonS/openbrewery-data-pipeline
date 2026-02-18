# OpenBrewery Data Pipeline

## 📌 Sobre o projeto

Este projeto implementa um **pipeline de dados usando dbt** para processar dados da Open Brewery API seguindo uma arquitetura em camadas (**Bronze → Silver → Gold**).

O objetivo é:
- Transformar dados brutos em **dados analíticos confiáveis**
- Aplicar boas práticas de **modelagem, testes e documentação**
- Disponibilizar uma camada **Gold pronta para consumo analítico**

A camada **Gold** agrega e organiza os dados para facilitar análises como:
- Quantidade de cervejarias por estado
- Distribuição por tipo de cervejaria

---

## 🧱 Estrutura do projeto

```text
dbt/
└── openbrewery/
    ├── models/
    │   ├── silver/
    │   └── gold/
    ├── tests/
    ├── macros/
    ├── dbt_project.yml
└── profiles.yml
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
