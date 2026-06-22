"""
S30 #1 live proof: PiFlowSkip -> task 'skipped' (NOT failed) + downstream skip-propagation.

skip_task raises PiFlowSkip; pre-fix this became 'failed' (burned retries, fired
on_failure_callback). Post-fix it must end 'skipped', and the all_success
downstream must skip-propagate to 'skipped' (Cat-2). control_ok is an unrelated
success task to show normal Python execution is unaffected.

Trigger: POST /api/dag-runs {"dag_id":"py1_skip_demo"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, PiFlowSkip


def skip_me(**context):
    raise PiFlowSkip("py1: nothing to do today")


def ran(**context):
    return "ran"


with DAG(
    dag_id="py1_skip_demo",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S30 #1: PiFlowSkip -> skipped + skip-propagation",
    tags=["s30", "py1", "skip"],
) as dag:
    skip_task = PythonOperator(task_id="skip_task", python_callable=skip_me)
    downstream = PythonOperator(
        task_id="downstream", python_callable=ran, trigger_rule="all_success"
    )
    control_ok = PythonOperator(task_id="control_ok", python_callable=ran)

    skip_task >> downstream
