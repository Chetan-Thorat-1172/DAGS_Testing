"""
Regression (Cat-2 / L2-06 + L1-03): branch skip propagation.

branch always picks path_a -> path_b is SKIPPED.
Then two joins depend on BOTH path_a (success) and path_b (skipped):
  - join_all_success (all_success): one upstream skipped => must end 'skipped'
    (Airflow skip-propagation). PRE-FIX PI-FLOW wrongly marked 'upstream_failed'.
  - join_nfmos (none_failed_min_one_success): one success + one skipped, no
    failures => must RUN ('success').

Trigger manually: POST /api/dag-runs {"dag_id":"rt_skip_propagation"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator


def choose(**context):
    return "path_a"  # deterministic: path_b never selected -> skipped


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_skip_propagation",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: branch skip -> all_success join SKIPPED, nfmos join RUNS",
    tags=["regression", "cat2", "skip"],
) as dag:
    branch = BranchPythonOperator(task_id="branch", python_callable=choose)
    path_a = PythonOperator(task_id="path_a", python_callable=ok)
    path_b = PythonOperator(task_id="path_b", python_callable=ok)
    join_all_success = PythonOperator(
        task_id="join_all_success", python_callable=ok, trigger_rule="all_success"
    )
    join_nfmos = PythonOperator(
        task_id="join_nfmos", python_callable=ok, trigger_rule="none_failed_min_one_success"
    )

    branch >> [path_a, path_b]
    [path_a, path_b] >> join_all_success
    [path_a, path_b] >> join_nfmos
