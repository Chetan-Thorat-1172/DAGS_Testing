"""
Regression (Cat-2 / L1-00): 'all_done_setup_success' rule, setup-FAILS path.

setup_fail (is_setup=True) fails -> finalize (all_done_setup_success) must be
SKIPPED (a required setup did not succeed), NOT upstream_failed.

Trigger manually: POST /api/dag-runs {"dag_id":"rt_setup_fail"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def boom(**context):
    raise Exception("intentional setup failure for all_done_setup_success regression")


with DAG(
    dag_id="rt_setup_fail",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: all_done_setup_success SKIPS when a setup upstream failed",
    tags=["regression", "cat2", "setup"],
) as dag:
    setup_fail = PythonOperator(task_id="setup_fail", python_callable=boom, retries=0, is_setup=True)
    finalize = PythonOperator(
        task_id="finalize", python_callable=ok, trigger_rule="all_done_setup_success"
    )

    setup_fail >> finalize
