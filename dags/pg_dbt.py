from datetime import timedelta
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

dbt_dataset = Dataset("dbt_load")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1, 0, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "scheduled_dbt_pg_daily",
    default_args=default_args,
    catchup=False,
    schedule=[dbt_dataset],
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /usr/local/airflow/include/dbt/dbt_project; source /usr/local/airflow/venv/bin/activate; dbt run --profiles-dir /usr/local/airflow/include/dbt/dbt_project/dbt-profiles/",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /usr/local/airflow/include/dbt/dbt_project; source /usr/local/airflow/venv/bin/activate; dbt test --profiles-dir /usr/local/airflow/include/dbt/dbt_project/dbt-profiles/",
    dag=dag,
)

with dag:
    dbt_run >> dbt_test