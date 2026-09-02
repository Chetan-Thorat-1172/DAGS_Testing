"""Feature 21 - Per-task callbacks.

A callback is 'tell someone what happened to THIS task', without adding a task
to the graph. Four events: success, retry, failure, skipped.

  succeeds  -> fires on_success
  flaky     -> fails once (fires on_retry), then fails for good (fires on_failure)
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator

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
        params={
            "_callbacks": {
                "on_success_callback": {
                    "type": "email",
                    "to": TO,
                    "subject": "OK: {{dag_id}}.{{task_id}}",
                    "html_content": "<p>Run {{run_id}} finished on {{event}}.</p>",
                },
            }
        },
    )

    flaky = PythonOperator(
        task_id="flaky",
        python_callable=always_fails,
        provide_context=True,
        retries=1,                  # 2 attempts: one retry, then final failure
        retry_delay_seconds=5,
        params={
            "_callbacks": {
                "on_retry_callback": {
                    "type": "email",
                    "to": TO,
                    "subject": "RETRYING: {{dag_id}}.{{task_id}}",
                    "html_content": "<p>Attempt failed, trying again.</p>",
                },
                "on_failure_callback": {
                    "type": "email",
                    "to": TO,
                    "subject": "FAILED: {{dag_id}}.{{task_id}}",
                    "html_content": "<p>Run {{run_id}} gave up.</p>",
                },
            }
        },
    )
