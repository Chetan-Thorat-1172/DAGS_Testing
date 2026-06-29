"""
s43_on_execute.py — LIVE proof for Fork (e2): on_execute_callback fires.

on_execute_callback is parsed/stored on operators but was never fired by the
worker. After the fix the worker enqueues a callback_request (callback_type=
'on_execute_callback') immediately BEFORE each execution attempt (once per try).

Two tasks:
  - exec_once (retries=0, succeeds): expect exactly 1 on_execute_callback row.
  - exec_twice (retries=1, fails on try 1, succeeds on try 2): expect exactly
    2 on_execute_callback rows -> proves once-per-try (fires again on retry).

Verify via callback_request table filtered by callback_type='on_execute_callback'
and worker log line "enqueued task callback request ... event=on_execute_callback".
The http_webhook target is harmless (httpbin); the row is enqueued regardless of
whether delivery succeeds.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator

# NOTE: callback body is deliberately STATIC (no {{ }}). PI-FLOW renders task
# params through Go's template engine BEFORE dispatch, and a Jinja-style
# {{dag_id}} is parsed as a Go-template function call -> the task fails to
# render. The on_execute_callback row enqueue does not depend on body content.
ON_EXECUTE = {
    "on_execute_callback": {
        "type": "http_webhook",
        "url": "https://httpbin.org/post",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": "{\"event\": \"on_execute\"}",
    }
}


def succeed(**context):
    print("[exec_once] running")
    return "ok"


def fail_then_succeed(**context):
    ti = context.get("ti")
    print(f"[exec_twice] try_number={ti.try_number}")
    if ti.try_number < 2:
        raise RuntimeError("intentional first-attempt failure (drives a retry)")
    return "recovered"


with DAG(
    dag_id="s43_on_execute2",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["s43", "fork-e", "on_execute"],
) as dag:
    PythonOperator(
        task_id="exec_once",
        python_callable=succeed,
        retries=0,
        params={"_callbacks": ON_EXECUTE},
    )
    PythonOperator(
        task_id="exec_twice",
        python_callable=fail_then_succeed,
        retries=1,
        params={"_callbacks": ON_EXECUTE},
    )
