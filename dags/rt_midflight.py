"""
Regression: mid-flight DAG version pinning.

VERSION 2: start >> slow >> finish, finish >> extra   (NEW task 'extra')

The in-flight run 'mf_run1' was triggered on v1 and must finish using v1:
instances = {start, slow, finish}, NO 'extra'. The latest version becomes v2
(includes 'extra'); only NEW runs / cleared tasks would pick it up.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def slow(**context):
    import time
    time.sleep(150)
    return "slow_done"


with DAG(
    dag_id="rt_midflight",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: mid-flight version pinning (v2: +extra)",
    tags=["regression", "cat2", "versioning"],
) as dag:
    start = PythonOperator(task_id="start", python_callable=ok)
    slow_t = PythonOperator(task_id="slow", python_callable=slow)
    finish = PythonOperator(task_id="finish", python_callable=ok)
    extra = PythonOperator(task_id="extra", python_callable=ok)  # NEW in v2

    start >> slow_t >> finish
    finish >> extra  # NEW edge
