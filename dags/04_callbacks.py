"""Feature 21 - Per-task callbacks.

A callback answers 'tell someone what happened to THIS task', without adding a
task to the graph. Four events: success, retry, failure, skipped.

  succeeds  -> fires on_success_callback
  flaky     -> fails once (fires on_retry_callback),
               then fails for good (fires on_failure_callback ONCE)

Note the callbacks are passed as operator kwargs, NOT inside params={...}.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, SmtpNotifier

TO = ["thoratc146@gmail.com"]


def ok():
    print("all good", flush=True)


def always_fails(**context):
    raise RuntimeError(f"failing on attempt {context['try_number']}")


with DAG(
    dag_id="04_callbacks",
    description="Feature 21 - per-task callbacks",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
) as dag:

    succeeds = PythonOperator(
        task_id="succeeds",
        python_callable=ok,
        on_success_callback=SmtpNotifier(
            to=TO,
            subject="OK - 04_callbacks.succeeds",
            html_content="<p>The succeeds task finished cleanly.</p>",
        ),
    )

    flaky = PythonOperator(
        task_id="flaky",
        python_callable=always_fails,
        provide_context=True,
        retries=1,                  # 2 attempts: one retry, then final failure
        retry_delay_seconds=5,
        on_retry_callback=SmtpNotifier(
            to=TO,
            subject="RETRYING - 04_callbacks.flaky",
            html_content="<p>An attempt failed, trying again.</p>",
        ),
        on_failure_callback=SmtpNotifier(
            to=TO,
            subject="FAILED - 04_callbacks.flaky",
            html_content="<p>The flaky task exhausted its retries.</p>",
        ),
    )
