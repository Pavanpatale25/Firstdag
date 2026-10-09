from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

MYSQL_CONN_ID = "mysql_assignment"
PIPELINE_NAME = "customer_summary_etl"
SQL_DIR = Path("/opt/airflow/sql")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def read_sql(filename):
    return (SQL_DIR / filename).read_text()


def extract_new_orders(**context):
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    watermark = hook.get_first(
        """
        SELECT last_processed_at
        FROM control.pipeline_watermark
        WHERE pipeline_name = %s
        """,
        parameters=(PIPELINE_NAME,),
    )

    if not watermark or watermark[0] is None:
        raise ValueError("Watermark not found.")

    rows = hook.get_records(read_sql("extract_new_orders.sql"))

    if not rows:
        logging.info("No new orders found.")
        return {"has_data": False, "upper_watermark": None}

    upper_watermark = max(row[4] for row in rows)

    logging.info(
        "Processed watermark range: %s to %s",
        watermark[0],
        upper_watermark,
    )

    return {
        "has_data": True,
        "upper_watermark": upper_watermark.strftime("%Y-%m-%d %H:%M:%S"),
    }


def transform_customer_summary():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    rows = hook.get_records(
        read_sql("transform_customer_summary.sql")
    )
    logging.info("Transformed %d rows.", len(rows))


def validate_data():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    rows = hook.get_records(read_sql("validate_data.sql"))

    if rows:
        raise ValueError(
            f"Data quality validation failed. Invalid rows: {rows}"
        )

    logging.info("Data quality validation passed.")


def load_customer_summary():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(read_sql("load_customer_summary.sql"))
    logging.info("Customer summary loaded successfully.")


def update_watermark(**context):
    data = context["ti"].xcom_pull(task_ids="extract_new_orders")

    if not data or not data["has_data"]:
        return

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    hook.run(
        read_sql("update_watermark.sql"),
        parameters=(data["upper_watermark"],),
    )

    logging.info(
        "Watermark updated to %s",
        data["upper_watermark"],
    )


with DAG(
    dag_id="customer_summary_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract_new_orders,
    )

    transform_task = PythonOperator(
        task_id="transform_customer_summary",
        python_callable=transform_customer_summary,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    load_task = PythonOperator(
        task_id="load_customer_summary",
        python_callable=load_customer_summary,
    )

    watermark_task = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    extract_task >> transform_task >> validate_task >> load_task >> watermark_task