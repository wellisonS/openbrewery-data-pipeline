FROM apache/airflow:2.8.1-python3.10

USER root

# dbt + Databricks provider
RUN pip install --no-cache-dir \
    dbt-databricks==1.7.0 \
    apache-airflow-providers-databricks

USER airflow
