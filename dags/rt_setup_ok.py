"""
Regression (Cat-2 / L1-00): 'all_done_setup_success' rule, setup-SUCCEEDS path.

setup_ok (is_setup=True) succeeds -> finalize (all_done_setup_success) must RUN.

Trigger manually: POST /api/dag-runs {"dag_id":"rt_setup_ok"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_setup_ok",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: all_done_setup_success runs when setup upstream succeeded",
    tags=["regression", "cat2", "setup"],
) as dag:
    setup_ok = PythonOperator(task_id="setup_ok", python_callable=ok, is_setup=True)
    work = PythonOperator(task_id="work", python_callable=ok)
    finalize = PythonOperator(
        task_id="finalize", python_callable=ok, trigger_rule="all_done_setup_success"
    )

    setup_ok >> work
    [setup_ok, work] >> finalize
