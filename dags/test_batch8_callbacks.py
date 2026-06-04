"""
test_batch8_callbacks.py — E2E test DAG for Batch 8 features.

Tests:
  1. Task-Level Callbacks (#21): on_failure_callback sends email, on_success_callback fires HTTP webhook
  2. Alerting Beyond Email (#12): HTTP webhook channel receives JSON payload on success

Flow:
  intentional_failure (fails → triggers email callback)
  webhook_on_success (succeeds → triggers HTTP webhook callback)

Usage:
  Trigger via API: POST /api/dag-runs { "dag_id": "test_batch8_callbacks", "conf": {} }
  Expected:
    - intentional_failure: FAILS, email sent to thoratc146@gmail.com within ~5s
    - webhook_on_success: SUCCEEDS, HTTP POST sent to httpbin.org/post
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime


def task_that_fails(**kwargs):
    """Intentionally fails to trigger on_failure_callback."""
    print("[test] About to raise an intentional error to test callbacks...")
    raise RuntimeError("Intentional failure to test task-level callback (#21)")


def task_that_succeeds(**kwargs):
    """Succeeds to trigger on_success_callback via HTTP webhook."""
    print("[test] Task completed successfully — webhook callback should fire.")
    return "batch8_callback_test_passed"


with DAG(
    dag_id="test_batch8_callbacks",
    schedule_interval="@once",  # Auto-trigger once for testing
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="E2E test for Batch 8: Task-Level Callbacks (#21) + Alerting Beyond Email (#12)",
    tags=["test", "batch8", "callbacks", "alerting"],
) as dag:

    t1 = PythonOperator(
        task_id="intentional_failure",
        python_callable=task_that_fails,
        retries=0,
        params={
            "_callbacks": {
                "on_failure_callback": {
                    "type": "email",
                    "to": ["thoratc146@gmail.com"],
                    "subject": "[PiFlow] Task {{task_id}} FAILED in {{dag_id}}",
                    "html_content": "<h2>Task Failure Alert</h2><p><b>DAG:</b> {{dag_id}}</p><p><b>Task:</b> {{task_id}}</p><p><b>Run:</b> {{run_id}}</p><p><b>Event:</b> {{event}}</p><p>This email was sent by the PiFlow Callback Dispatcher (Batch 8, Feature #21).</p>"
                }
            }
        },
    )

    t2 = PythonOperator(
        task_id="webhook_on_success",
        python_callable=task_that_succeeds,
        params={
            "_callbacks": {
                "on_success_callback": {
                    "type": "http_webhook",
                    "url": "https://httpbin.org/post",
                    "method": "POST",
                    "headers": {"X-Source": "piflow-batch8-test", "Content-Type": "application/json"},
                    "body": "{\"dag_id\": \"{{dag_id}}\", \"task_id\": \"{{task_id}}\", \"run_id\": \"{{run_id}}\", \"event\": \"{{event}}\", \"message\": \"Batch 8 callback test successful\"}"
                }
            }
        },
    )
