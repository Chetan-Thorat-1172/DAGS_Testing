"""
Regression: mid-flight DAG version pinning — definitive test on a FRESH dag_id.

Fresh dag_id (no version history) so the real ingester produces correct version
ordering with zero manual DB edits.

VERSION 1 (this file): start >> slow(300s) >> finish   (3 tasks, no 'extra')
VERSION 2 (pushed after run1's slow is executing):
    start >> slow(0s) >> finish >> extra                (4 tasks)
    - 'slow' kept in both versions (avoids fk_ti_task cascade-delete)
    - slow=0 in v2 so run2 finishes instantly
    - run1 must complete as {start,slow,finish}; run2 as {start,slow,finish,extra}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(300)   # v1: 5-minute mid-flight window
    return "slow_done"


with DAG(
    dag_id="rt_midflight_final",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="mid-flight final v1: start>>slow(300)>>finish",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start  = PythonOperator(task_id="start",  python_callable=ok)
    slow_t = PythonOperator(task_id="slow",   python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)

    start >> slow_t >> finish
