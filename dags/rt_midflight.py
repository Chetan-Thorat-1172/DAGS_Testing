"""
Regression: mid-flight DAG version pinning.

VERSION 1 (baseline): start >> slow(300s) >> finish
  - No 'extra' task.
  - The 5-minute slow window lets us ingest v2 while run1 is still in-flight.

VERSION 2 (will be pushed mid-flight):
  - start >> finish >> extra  (slow removed; run2 finishes instantly)
  - run1 must ignore 'extra' and complete as {start, slow, finish}.
  - run2 must use v2: {start, finish, extra}.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(300)   # 5-minute window — gives ingestion plenty of time
    return "slow_done"


with DAG(
    dag_id="rt_midflight",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: mid-flight version pinning (v1: start>>slow300>>finish)",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start  = PythonOperator(task_id="start",  python_callable=ok)
    slow_t = PythonOperator(task_id="slow",   python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)

    start >> slow_t >> finish
