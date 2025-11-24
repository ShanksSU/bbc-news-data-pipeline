from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_hello():
    print("Hello from Airflow test DAG!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="test_connection_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["jingliu", "is", "gorgeous"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=_print_hello,
    )
