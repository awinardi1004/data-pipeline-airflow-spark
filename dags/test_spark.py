from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="test_spark_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_spark = SparkSubmitOperator(
        task_id="test_spark",
        application="/opt/airflow/dags/pi.py",
        conn_id="spark_default",
        verbose=True,
    )

    test_spark
