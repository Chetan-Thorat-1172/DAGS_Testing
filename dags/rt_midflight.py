"""
Regression: mid-flight DAG version pinning — final definitive test.

VERSION 1 (this file): start >> slow(300s) >> finish
  - 3 tasks. No 'extra'. slow=300s gives a 5-minute mid-flight window.

VERSION 2 (pushed after run1 is executing slow):
  - start >> slow(0s) >> finish >> extra
  - slow is KEPT (avoids cascade-delete of run1's slow instance).
  - slow=0 makes run2 finish instantly.
  - 'extra' is the new task — run1 must NOT get it, run2 MUST.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(300)   # v1: 5-minute window for v2 to be ingested mid-flight
    return "slow_done"


with DAG(
    dag_id="rt_midflight",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="mid-flight v1: start>>slow(300)>>finish",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start  = PythonOperator(task_id="start",  python_callable=ok)
    slow_t = PythonOperator(task_id="slow",   python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)

    start >> slow_t >> finish
