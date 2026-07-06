from datetime import datetime
import time

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def slow(**ctx):
    time.sleep(30)
    return "done"


with DAG(
    dag_id="test_task_sla",
    schedule=None,
    start_date=datetime(2026, 7, 6),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="slow",
        python_callable=slow,
        sla=5,  # 5-second per-task SLA
    )
