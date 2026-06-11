"""
Test DAG: @once Auto-Trigger (Session 2 Scheduler Fix)

Validates that @once DAGs automatically get a single run created
at execution_date=start_date without manual intervention.

Expected: scheduler creates one run with execution_date=2026-01-01T00:00:00,
state transitions queued → running → success.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_once_autotrigger",
    schedule_interval="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Validates @once auto-trigger fix (Session 2)",
    tags=["test", "scheduler", "once"],
) as dag:

    verify = BashOperator(
        task_id="verify_once_triggered",
        bash_command="echo 'once_auto_triggered_successfully'",
    )
