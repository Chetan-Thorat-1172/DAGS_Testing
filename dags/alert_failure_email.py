"""alert_failure_email.py — Test DAG-LEVEL on_failure_callback (email).

What it tests:
  - A DAG-level on_failure_callback=SmtpNotifier(...) firing when the run fails.
  - The failing task ('boom') exits non-zero so the run reaches the 'failed' state,
    which is what triggers the DAG-level on_failure_callback in dag_run_finalizer.

How to use:
  1. Set the recipient(s) in SmtpNotifier(to=...).
  2. Deploy, let it run (or trigger manually). The 'boom' task fails -> run fails.

Delivery prerequisites (server-side):
  - SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASSWORD/SMTP_FROM must be configured.
  - The callback dispatcher must consume DAG-authored callback configs (see notes
    from the team — DAG-driven SmtpNotifier callbacks may require a dispatcher fix).

Subject/body support {{ dag_id }}, {{ task_id }}, {{ run_id }}, {{ event }} placeholders
(substituted by the dispatcher at send time).
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SmtpNotifier

with DAG(
    dag_id="alert_failure_email",
    schedule=None,                      # manual trigger
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test"],
    on_failure_callback=SmtpNotifier(
        to=["Chetan.Thorat@Pibythree.com"],
        subject="[PiFlow] DAG {{ dag_id }} FAILED (run {{ run_id }})",
        html_content="DAG <b>{{ dag_id }}</b> failed on run {{ run_id }}. Event: {{ event }}.",
    ),
) as dag:

    start = BashOperator(task_id="start", bash_command="echo starting")
    boom = BashOperator(task_id="boom", bash_command="echo about to fail && exit 1")

    start >> boom
