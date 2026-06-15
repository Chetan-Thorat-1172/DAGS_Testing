"""alert_success_email.py — Test DAG-LEVEL on_success_callback (email).

What it tests:
  - A DAG-level on_success_callback=SmtpNotifier(...) firing when the run succeeds.
  - All tasks succeed -> run reaches 'success' -> dag_run_finalizer fires
    on_success_callback.

Set the recipient and see the success notification. Same SMTP/dispatcher prerequisites
as alert_failure_email.py apply.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SmtpNotifier

with DAG(
    dag_id="alert_success_email",
    schedule=None,                      
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test"],
    on_success_callback=SmtpNotifier(
        to=["Chetan.Thorat@Pibythree.com"],      
        subject="[PiFlow] DAG {{ dag_id }} SUCCEEDED (run {{ run_id }})",
        html_content="DAG <b>{{ dag_id }}</b> completed successfully (run {{ run_id }}).",
    ),
) as dag:

    a = BashOperator(task_id="step_a", bash_command="echo a")
    b = BashOperator(task_id="step_b", bash_command="echo b")

    a >> b
