"""
s43_on_execute.py - LIVE proof for Fork (e2): on_execute_callback fires.

on_execute_callback is parsed/stored on operators but was never fired by the
worker. After the fix the worker enqueues a callback_request (callback_type=
'on_execute_callback') immediately BEFORE each execution attempt (once per try).

Callbacks are specified via the operator KWARG (on_execute_callback=<fn>), which
the parser hoists to TOP-LEVEL params._callbacks (sandbox_runner) - the location
fireTaskCallbacks reads. (params={"_callbacks":...} instead nests under
params.params._callbacks and is NOT read - that was the earlier mistake.)

Two tasks:
  - exec_once (retries=0, succeeds): expect exactly 1 on_execute_callback row.
  - exec_twice (retries=1, fails on try 1, succeeds on try 2): expect exactly
    2 on_execute_callback rows -> proves once-per-try (fires again on retry).

Verify via callback_request table filtered by callback_type='on_execute_callback'.
The row enqueue is the proof; the callback is a function (the dispatcher need not
deliver it).
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def on_execute_notify(*args, **kwargs):
    """Task-level on_execute callback (serialized as {type:function} by the parser)."""
    print("[callback] on_execute fired")


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
    dag_id="s43_on_execute3",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["s43", "fork-e", "on_execute"],
) as dag:
    PythonOperator(
        task_id="exec_once",
        python_callable=succeed,
        retries=0,
        on_execute_callback=on_execute_notify,
    )
    PythonOperator(
        task_id="exec_twice",
        python_callable=fail_then_succeed,
        retries=1,
        on_execute_callback=on_execute_notify,
    )
