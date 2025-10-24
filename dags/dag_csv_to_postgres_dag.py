from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "postgres", "staging"]
)
def load_csv_to_postgres():

    spark_load = SparkSubmitOperator(
        task_id="spark_load_csv_to_postgres",
        conn_id="spark_conn",
        application="/opt/airflow/include/scripts/load_csv_to_postgres.py",
        verbose=True,
    )

    spark_load

load_csv_to_postgres()
