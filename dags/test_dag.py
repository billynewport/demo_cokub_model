from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_gitsync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    task = BashOperator(
        task_id="hello",
        bash_command="echo 'GitSync is working!'",
    )
