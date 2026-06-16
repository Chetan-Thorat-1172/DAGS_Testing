"""alert_emailoperator_test.py — Test EmailOperator as a task-based email alert.

HOW THIS DAG WORKS:
  - 'do_work' intentionally fails (exit 1)
  - 'email_on_failure' fires ONLY when do_work fails  (trigger_rule="one_failed")
  - 'email_on_success' fires ONLY when everything succeeds (trigger_rule="all_success")

  When you run this DAG:
    do_work FAILS → email_on_failure sends email → email_on_success is skipped

  To test the success path separately:
    Change 'do_work' bash_command to "echo ok" and re-deploy.

WHY THIS WORKS (unlike SmtpNotifier callbacks):
  EmailOperator runs through the WORKER EXECUTOR path (executor_email.go), not the
  callback dispatcher. The executor directly connects to SMTP and sends the email.
  No callback_request table involved — no dispatcher format issue.

PREREQUISITE:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM must be set on the server.
  If SMTP_HOST is not set, EmailOperator is not registered and the task will fail
  with "no executor found for operator: emailoperator".
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, EmailOperator

with DAG(
    dag_id="alert_emailoperator_test",
    schedule=None,                      # manual trigger only
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["alerting", "test", "email"],
) as dag:

    # Step 1 — the actual work task (intentionally fails to test failure email)
    do_work = BashOperator(
        task_id="do_work",
        bash_command="echo ok",   # change to 'echo ok' to test success path (echo starting work && exit 1)
    )

    # Step 2a — email fired ONLY when do_work (or any upstream) fails
    email_on_failure = EmailOperator(
        task_id="email_on_failure",
        to=["Chetan.Thorat@Pibythree.com"],
        subject="[PiFlow] {{ dag_id }} — task FAILED",
        html_content="""
            <h3>PI_FLOW Alert — Task Failure</h3>
            <p><b>DAG:</b> {{ dag_id }}</p>
            <p><b>Run:</b> {{ run_id }}</p>
            <p><b>Task:</b> {{ task_id }}</p>
            <p>One or more tasks failed. Please check the PI_FLOW UI for details.</p>
        """,
        trigger_rule="one_failed",      # runs if ANY upstream task failed
    )

    # Step 2b — email fired ONLY when everything succeeds
    email_on_success = EmailOperator(
        task_id="email_on_success",
        to=["Chetan.Thorat@Pibythree.com"],
        subject="[PiFlow] {{ dag_id }} — completed SUCCESSFULLY",
        html_content="""
            <h3>PI_FLOW Alert — Run Succeeded</h3>
            <p><b>DAG:</b> {{ dag_id }}</p>
            <p><b>Run:</b> {{ run_id }}</p>
            <p>All tasks completed successfully.</p>
        """,
        trigger_rule="all_success",     # runs only if ALL upstream tasks succeeded
    )

    # Wire: do_work → both email tasks
    do_work >> [email_on_failure, email_on_success]
