"""
Regression: mid-flight DAG version pinning — definitive test on a FRESH dag_id.

VERSION 2: start >> slow(0s) >> finish >> extra   (4 tasks)
  - 'slow' kept (no fk_ti_task cascade), now sleeps 0 so run2 is instant.
  - 'extra' is NEW.
  - run1 (triggered on v1) must finish as {start,slow,finish}, NO extra.
  - run2 (triggered on v2) must run {start,slow,finish,extra}.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(0)   # v2: instant so run2 completes immediately
    return "slow_done"


with DAG(
    dag_id="rt_midflight_final",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="mid-flight final v2: start>>slow(0)>>finish>>extra",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start  = PythonOperator(task_id="start",  python_callable=ok)
    slow_t = PythonOperator(task_id="slow",   python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)
    extra  = PythonOperator(task_id="extra",  python_callable=ok)  # NEW in v2

    start >> slow_t >> finish >> extra
