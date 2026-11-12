
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    rows = hook.get_records(read_sql("extract_new_orders.sql"))

    if not rows:
        logging.info("No new records found")
        return None

    watermark = max(row[4] for row in rows)

    if isinstance(watermark, datetime):
        watermark = watermark.isoformat(sep=" ")

    return {"record_count": len(rows), "watermark": watermark}


def check_new_records(ti):
    data = ti.xcom_pull(task_ids="extract_new_orders")
    return "validate_data" if data else "no_new_records"


def validate(ti):
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    rows = hook.get_records(read_sql("validate_data.sql"))

    invalid_records = [
        {
            "customer_id": row[0],
            "customer_name": row[1],
            "rejection_reason": row[2]
        }
        for row in rows
    ]

    ti.xcom_push(key="invalid_records", value=invalid_records)
    logging.info("Invalid records found: %s", len(invalid_records))


def reject_invalid_data(ti):
    rows = ti.xcom_pull(
        task_ids="validate_data",
        key="invalid_records"
    )

    if not rows:
        logging.info("No invalid records to reject")
        return

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    for row in rows:
        hook.run("""
            INSERT INTO control.rejected_customer_summary
            (customer_id, customer_name, rejection_reason)
            VALUES (%s, %s, %s)
        """, parameters=(
            row["customer_id"],
            row["customer_name"],
            row["rejection_reason"]
        ))


def transform_load_all():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    hook.run(read_sql("load_customer_summary.sql"))

    analytics_files = [
        "city_customer_count.sql",
        "monthly_top_customer.sql",
        "top_selling_month.sql",
        "monthly_unique_products.sql",
        "monthly_order_status.sql"
    ]

    for filename in analytics_files:
        hook.run(read_sql(filename))
        logging.info("%s completed", filename)


def update_watermark(ti):
    data = ti.xcom_pull(task_ids="extract_new_orders")

    if not data:
        return

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(
        read_sql("update_watermark.sql"),
        parameters=(data["watermark"],)
    )


with DAG(
    dag_id="customer_summary_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Customer Summary and Analytics ETL",
    tags=["assignment"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract
    )

    check_task = BranchPythonOperator(
        task_id="check_new_records",
        python_callable=check_new_records
    )

    no_records = EmptyOperator(task_id="no_new_records")

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate
    )

    reject_task = PythonOperator(
        task_id="reject_invalid_data",
        python_callable=reject_invalid_data
    )

    load_task = PythonOperator(
        task_id="transform_load_all",
        python_callable=transform_load_all
    )

    watermark_task = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark
    )

    success_email = EmailOperator(
        task_id="send_success_email",
        to="pavanpatale25@gmail.com",
        subject="Customer Summary ETL - Successful",
        html_content="""
            <h3>ETL Pipeline Completed Successfully!</h3>
            <p>Customer summary and analytics tables were updated.</p>
            <p>Watermark was updated successfully.</p>
        """
    )

    trigger_another_dag = TriggerDagRunOperator(
        task_id="trigger_another_dag",
        trigger_dag_id="another_dag"
    )

    extract_task >> check_task
    check_task >> [validate_task, no_records]
    validate_task >> [reject_task, load_task]
    [reject_task, load_task] >> watermark_task
    watermark_task >> success_email >> trigger_another_dag
