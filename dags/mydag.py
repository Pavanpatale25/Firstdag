
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pathlib import Path
from datetime import datetime, timedelta
import logging

MYSQL_CONN_ID = "mysql_assignment"
SQL_DIR = Path("/opt/airflow/sql")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def read_sql(filename):
    return (SQL_DIR / filename).read_text()


def extract():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    rows = hook.get_records(
        read_sql("extract_new_orders.sql")
    )

    if not rows:
        logging.info("No new orders found")
        return None

    # The fifth column contains updated_at.
    watermark = max(row[4] for row in rows)

    # Return a string so Airflow can store it in XCom.
    if isinstance(watermark, datetime):
        watermark = watermark.isoformat(sep=" ")

    logging.info("Extracted %s records", len(rows))
    return watermark


def validate():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    rows = hook.get_records(read_sql("validate_data.sql"))

    if rows:
        raise ValueError(f"Validation failed: {rows}")

    logging.info("Validation passed")


def transform():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(read_sql("transform_customer_summary.sql"))
    logging.info("Transformation completed")


def load():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(read_sql("load_customer_summary.sql"))
    logging.info("Customer summary loaded")


def update_watermark(**context):
    watermark = context["ti"].xcom_pull(
        task_ids="extract_new_orders"
    )

    if not watermark:
        logging.info("No new records; watermark unchanged")
        return

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    hook.run(
        read_sql("update_watermark.sql"),
        parameters=(watermark,),
    )

    logging.info("Watermark updated to %s", watermark)


with DAG(
    dag_id="customer_summary_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Simple Customer Summary ETL",
    tags=["assignment"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_customer_summary",
        python_callable=load,
    )

    watermark_task = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    (
        extract_task
        >> validate_task
        >> transform_task
        >> load_task
        >> watermark_task
    )
