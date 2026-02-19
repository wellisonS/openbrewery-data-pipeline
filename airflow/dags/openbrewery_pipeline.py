from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="openbrewery_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["openbrewery"],
) as dag:

    bronze = DatabricksRunNowOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        job_id=500628456012694
    )

    silver = DatabricksRunNowOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        job_id=246678071932823
    )

    gold = BashOperator(
        task_id="dbt_gold",
        bash_command="""
        cd /opt/dbt/openbrewery &&
        dbt run --select gold &&
        dbt test --select gold
        """
    )

    bronze >> silver >> gold