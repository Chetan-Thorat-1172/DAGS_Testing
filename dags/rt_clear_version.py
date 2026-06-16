"""
Regression (Cat-2 / L2-07): clear-task version reconciliation.

VERSION 1: a >> c   (c depends only on a)

Test protocol:
  1. (this version) run rt_clear_version to success.
  2. bump to VERSION 2 which ADDS task 'b' as a NEW upstream of 'c'
     (a >> c, b >> c). The completed run stays on v1's dag_hash.
  3. clear task 'c' on the v1 run via the UI.
     Expected (L2-07 fix): original_dag_hash is preserved (= v1), dag_hash is
     bumped to v2, a 'none' instance is reconciled for the NEW task 'b'
     (preventing the missing-upstream deadlock), then 'b' runs and 'c' re-runs
     under v2 -> run finalizes success.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_clear_version",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression L2-07: clear-task version reconciliation (v1: a>>c)",
    tags=["regression", "cat2", "clear"],
) as dag:
    a = PythonOperator(task_id="a", python_callable=ok)
    c = PythonOperator(task_id="c", python_callable=ok)

    a >> c
