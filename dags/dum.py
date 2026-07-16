"""alert_sla_miss_email.py — Test on_sla_miss_callback (email).

What it tests:
  - A DAG-level on_sla_miss_callback=SmtpNotifier(...) fired by the SLA monitor when a
    run exceeds expected_duration_seconds.
  - 'slow' sleeps far longer than expected_duration_seconds (10s), so the SLA monitor
    detects the breach and enqueues the SLA-miss callback.

Note: the SLA monitor wraps the callback config with SLA context; delivery still depends
on SMTP config and the dispatcher consuming the config (see team notes).
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SmtpNotifier

with DAG(
    dag_id="ert",
    schedule=None,
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test"],
    expected_duration_seconds=10,       # run is "late" if it takes longer than 10s
    on_sla_miss_callback=SmtpNotifier(
        to=["Chetan.Thorat@Pibythree.com"],
        subject="[PiFlow] SLA miss on {{ dag_id }} (run {{ run_id }})",
        html_content="DAG {{ dag_id }} exceeded its expected duration (run {{ run_id }}).",
    ),
) as dag:

    slow = BashOperator(task_id="slow", bash_command="sleep 120 && echo done")
