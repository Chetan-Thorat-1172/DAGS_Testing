"""
s43_reschedule.py — LIVE proof for Fork (e1): PiFlowReschedule.

A Python task voluntarily reschedules itself (Airflow reschedule-mode-sensor
semantics for arbitrary Python) instead of failing, until a time window elapses.

Self-recovering, no manual step:
  - anchor: records wall-clock epoch as its return_value XCom.
  - waiter: pulls anchor's epoch; while < 75s elapsed it raises
    PiFlowReschedule(delay=30) -> task goes 'up_for_reschedule', releases the
    worker, scheduler re-queues after ~30s (NOT now+0). After ~3 reschedules the
    window passes and the task returns normally -> 'success'.

Expected persisted state (task_instance):
  - waiter visits state 'up_for_reschedule' with next_retry_at ~= now()+30
  - try_number is NOT incremented by reschedules (reschedule != retry)
  - waiter ends 'success'; anchor 'success'
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, PiFlowReschedule

WINDOW_SECS = 75
RESCHEDULE_DELAY = 30


def anchor(**context):
    import time
    epoch = time.time()
    print(f"[anchor] recording start epoch={epoch:.3f}")
    return epoch


def waiter(**context):
    import time
    ti = context.get("ti")
    start = float(ti.xcom_pull(task_ids="anchor"))
    elapsed = time.time() - start
    print(f"[waiter] try_number={ti.try_number} elapsed={elapsed:.1f}s")
    if elapsed < WINDOW_SECS:
        raise PiFlowReschedule(
            f"waiting for window ({elapsed:.0f}/{WINDOW_SECS}s)",
            delay=RESCHEDULE_DELAY,
        )
    return f"recovered after {elapsed:.0f}s (try_number={ti.try_number})"


with DAG(
    dag_id="s43_reschedule2",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["s43", "fork-e", "reschedule"],
) as dag:
    a = PythonOperator(task_id="anchor", python_callable=anchor, retries=0)
    w = PythonOperator(task_id="waiter", python_callable=waiter, retries=0)
    a >> w
