"""alert_task_level_email.py — Test TASK-LEVEL on_failure / on_success callbacks (email).

What it tests:
  - Task-level on_failure_callback / on_success_callback=SmtpNotifier(...).
  - 'ok_task' succeeds  -> its on_success_callback fires.
  - 'fail_task' fails   -> its on_failure_callback fires (and the run then fails).

Task callbacks are fired by the worker (task_runner.fireTaskCallbacks) right after the
task reaches success/failure. Same SMTP/dispatcher prerequisites apply.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SmtpNotifier

with DAG(
    dag_id="alert_task_level_email",
    schedule=None,
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test"],
) as dag:

    ok_task = BashOperator(
        task_id="ok_task",
        bash_command="echo success path",
        on_success_callback=SmtpNotifier(
            to=["Chetan.Thorat@Pibythree.com"],
            subject="[PiFlow] task {{ task_id }} succeeded",
            html_content="Task {{ task_id }} in {{ dag_id }} succeeded (run {{ run_id }}).",
        ),
    )

    fail_task = BashOperator(
        task_id="fail_task",
        bash_command="echo failing now && exit 1",
        on_failure_callback=SmtpNotifier(
            to=["Chetan.Thorat@Pibythree.com"],
            subject="[PiFlow] task {{ task_id }} FAILED",
            html_content="Task {{ task_id }} in {{ dag_id }} failed (run {{ run_id }}).",
        ),
    )

    ok_task >> fail_task
