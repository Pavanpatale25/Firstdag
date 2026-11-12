from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_trigger_message():
    print("triggered another dag successfully")


with DAG(
    dag_id="another_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    print_task = PythonOperator(
        task_id="print_trigger_message",
        python_callable=print_trigger_message
    )
