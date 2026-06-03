"""
test_deferrable_operator.py — End-to-end test for Deferrable Operators (#72).

Tests the full self.defer() pipeline:
  1. Task 'setup' runs normally
  2. Task 'defer_with_timer' raises TaskDeferred(TimeDeltaTrigger(5))
     - Goes to 'deferred' state, triggerer fires after 5s
     - Resumes at execute_complete() → success
  3. Task 'after_defer' runs downstream
  4. Task 'verify_xcom' confirms the whole chain worked

Expected: ~8s total runtime.
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    TaskDeferred,
    TimeDeltaTrigger,
)


def setup_task(**context):
    """Simple setup task."""
    print("[setup] Running setup task")
    return "setup_done"


def deferrable_task(**context):
    """
    Raises TaskDeferred on first call.
    On resume (_resume_method=execute_complete), this function won't be called —
    instead execute_complete() below will be called directly.
    """
    print("[defer_with_timer] First execution — raising TaskDeferred")
    trigger = TimeDeltaTrigger(delta=5)
    raise TaskDeferred(trigger=trigger, method_name="execute_complete", timeout=60)


def execute_complete(event=None):
    """
    Resume method called by _run_task.py when the trigger fires.
    This is a module-level function that the resume path can find via:
      params._dag_module + getattr(module, 'execute_complete')
    """
    print(f"[defer_with_timer] Trigger fired! execute_complete called with event={event}")
    return "deferred_and_resumed_successfully"


def after_defer_task(**context):
    """Downstream task."""
    ti = context.get("ti")
    defer_result = ti.xcom_pull(task_ids="defer_with_timer", key="return_value")
    print(f"[after_defer] defer_with_timer result: {defer_result}")
    return "downstream_done"


def verify_xcom_task(**context):
    """Final verification."""
    ti = context.get("ti")
    setup_val = ti.xcom_pull(task_ids="setup", key="return_value")
    defer_val = ti.xcom_pull(task_ids="defer_with_timer", key="return_value")
    after_val = ti.xcom_pull(task_ids="after_defer", key="return_value")
    print(f"[verify_xcom] setup={setup_val}, defer={defer_val}, after={after_val}")

    if not defer_val:
        raise ValueError(f"Missing XCom from defer_with_timer! Got: {defer_val}")
    print("[verify_xcom] ALL VERIFIED — deferrable operator pipeline complete!")
    return "all_verified"


with DAG(
    dag_id="test_deferrable_operator",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="E2E test: deferrable operators (#72) — self.defer() + triggerer + resume",
    tags=["test", "deferrable", "batch4"],
) as dag:

    setup = PythonOperator(
        task_id="setup",
        python_callable=setup_task,
    )

    defer_with_timer = PythonOperator(
        task_id="defer_with_timer",
        python_callable=deferrable_task,
    )

    after_defer = PythonOperator(
        task_id="after_defer",
        python_callable=after_defer_task,
    )

    verify_xcom = PythonOperator(
        task_id="verify_xcom",
        python_callable=verify_xcom_task,
    )

    setup >> defer_with_timer >> after_defer >> verify_xcom
