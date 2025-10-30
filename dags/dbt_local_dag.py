from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "winardi",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


DBT_PROJECT_DIR = "/opt/airflow/include/dbt/dbt_project"
DBT_PROFILE_DIR = "/home/airflow/.dbt"

with DAG(
    dag_id="dag_dbt_pipeline",
    default_args=default_args,
    description="Run and test dbt project inside Airflow container",
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "local", "pipeline"],
) as dag:

    # Task 1: dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --profiles-dir {DBT_PROFILE_DIR} --target dev
        """,
    )

    # Task 2: dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --profiles-dir {DBT_PROFILE_DIR} --target dev
        """,
    )

    dbt_run >> dbt_test
