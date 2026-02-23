FROM apache/airflow:2.8.1-python3.10

RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks==6.0.0 \
    dbt-databricks==1.7.0 \
    databricks-sdk==0.20.0