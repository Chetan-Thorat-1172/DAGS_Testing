"""
s43_resched_timeout.py - LIVE proof for #5: PiFlowReschedule execution_timeout bound.

The waiter ALWAYS raises PiFlowReschedule(delay=20) - it would reschedule forever.
With execution_timeout=60, the worker must FAIL it once total wait since the first
attempt exceeds 60s, instead of rescheduling indefinitely.

Expected persisted state (task_instance):
  - waiter reschedules at ~20s intervals (next_retry_at honored), try_number stays 1
    (reschedule != retry) -> this also re-confirms the S43 e1 reschedule path.
  - after ~60-80s the worker fails it: state='failed', log/error
    "reschedule wait exceeded execution_timeout (60s)".
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, PiFlowReschedule


def always_wait(**context):
    ti = context.get("ti")
    raise PiFlowReschedule(f"always waiting (try_number={ti.try_number})", delay=20)


with DAG(
    dag_id="s43_resched_timeout",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["s43", "fork-e", "reschedule-timeout"],
) as dag:
    PythonOperator(
        task_id="waiter",
        python_callable=always_wait,
        retries=0,
        execution_timeout=60,
    )
