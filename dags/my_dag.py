from airflow.decorators import dag, task
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
    
    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id="spark_conn",
        application="/opt/airflow/include/scripts/read.py",
        verbose=True,
    )
    
    submit_job

my_dag()