"""
Regression: mid-flight DAG version pinning.

VERSION 2: start >> finish >> extra  (slow task REMOVED — run2 finishes instantly)

run1 started on v1 {start,slow,finish} must complete with those 3 tasks only.
run2 started on v2 must pick up {start,finish,extra}.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_midflight",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: mid-flight version pinning (v2: start>>finish>>extra)",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start  = PythonOperator(task_id="start",  python_callable=ok)
    finish = PythonOperator(task_id="finish", python_callable=ok)
    extra  = PythonOperator(task_id="extra",  python_callable=ok)  # NEW in v2

    start >> finish >> extra
