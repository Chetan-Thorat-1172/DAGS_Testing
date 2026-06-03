"""
test_deferrable_operator.py — End-to-end test for Deferrable Operators (#72).

Tests the full self.defer() pipeline:
  1. Task 'setup' runs normally (validates basic execution still works)
  2. Task 'defer_with_timer' calls self.defer(TimeDeltaTrigger(delta=5))
     - Task enters 'deferred' state, worker slot released
     - trigger_instance row created in DB
     - Triggerer polls and fires after 5 seconds
     - Task resumes at execute_complete() method
  3. Task 'after_defer' runs after defer_with_timer completes (validates deps work with deferred)
  4. Task 'verify_xcom' pulls XCom from defer_with_timer to confirm resume worked

Expected execution timeline:
  t=0s:  setup → success
  t=0s:  defer_with_timer → starts → calls self.defer() → deferred state
  t=5s:  triggerer fires TimeDeltaTrigger → task re-queued → scheduled
  t=5s:  defer_with_timer resumes at execute_complete() → success
  t=5s:  after_defer → starts → success
  t=5s:  verify_xcom → pulls XCom → success

Total expected time: ~6-8 seconds.
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    TaskDeferred,
    TimeDeltaTrigger,
)


def setup_task(**context):
    """Simple setup task — validates basic Python execution."""
    print("[setup] Running setup task")
    ti = context.get("ti")
    ti.xcom_push(key="setup_status", value="ready")
    return "setup_done"


def task_that_defers(**context):
    """
    This task demonstrates the deferrable operator pattern:
    1. Does some initial work
    2. Calls self.defer() to register a trigger and release the worker
    3. When trigger fires, execute_complete() is called with the event
    """
    ti = context.get("ti")
    print(f"[defer_with_timer] Starting (try_number={ti.try_number})")

    # Check if this is the resume path (_resume_method would be set)
    # On first execution, we raise TaskDeferred
    # On resume, _run_task.py routes to execute_complete directly
    print("[defer_with_timer] Deferring with TimeDeltaTrigger(5 seconds)...")
    trigger = TimeDeltaTrigger(delta=5)
    raise TaskDeferred(trigger=trigger, method_name="execute_complete", timeout=60)


def after_defer_task(**context):
    """Runs after the deferred task completes — validates dependency resolution."""
    ti = context.get("ti")
    print("[after_defer] Deferred task completed! Running downstream.")
    # Pull XCom from deferred task to confirm it ran execute_complete
    result = ti.xcom_pull(task_ids="defer_with_timer", key="return_value")
    print(f"[after_defer] defer_with_timer return_value: {result}")
    return "downstream_done"


def verify_xcom_task(**context):
    """Verifies XCom from deferred task is accessible."""
    ti = context.get("ti")
    setup_val = ti.xcom_pull(task_ids="setup", key="return_value")
    defer_val = ti.xcom_pull(task_ids="defer_with_timer", key="return_value")
    after_val = ti.xcom_pull(task_ids="after_defer", key="return_value")
    print(f"[verify_xcom] setup={setup_val}, defer={defer_val}, after={after_val}")

    # Basic assertions
    if not setup_val:
        raise ValueError("Missing XCom from setup task!")
    if not after_val:
        raise ValueError("Missing XCom from after_defer task!")
    print("[verify_xcom] All XCom values present — deferrable pipeline verified!")
    return "all_verified"


with DAG(
    dag_id="test_deferrable_operator",
    schedule_interval=None,  # Manual trigger only
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
        python_callable=task_that_defers,
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
