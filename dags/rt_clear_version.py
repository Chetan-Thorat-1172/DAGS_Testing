"""
Regression (Cat-2 / L2-07): clear-task version reconciliation.

VERSION 2: a >> c, b >> c   (NEW task 'b' is an added upstream of 'c')

The completed run 'clearver_run1' is still on v1's dag_hash. Clearing task 'c'
on that run must (L2-07 fix): preserve original_dag_hash (= v1), bump dag_hash
to v2, reconcile a 'none' instance for the NEW task 'b' (no missing-upstream
deadlock), then run 'b' and re-run 'c' under v2 -> run finalizes success.
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
    description="Regression L2-07: clear-task version reconciliation (v2: a>>c, b>>c)",
    tags=["regression", "cat2", "clear"],
) as dag:
    a = PythonOperator(task_id="a", python_callable=ok)
    b = PythonOperator(task_id="b", python_callable=ok)  # NEW in v2
    c = PythonOperator(task_id="c", python_callable=ok)

    a >> c
    b >> c  # NEW edge: b is an added upstream of c
