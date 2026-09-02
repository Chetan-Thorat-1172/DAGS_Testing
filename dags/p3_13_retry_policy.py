"""
Knowledge session — Part 3, Feature 13: Retry policy.

Two tasks, side by side, so the grid view tells the whole story:

  succeeds_on_attempt_3  ->  fails twice, then succeeds. Watch the state go
                             running -> up_for_retry -> running -> up_for_retry
                             -> running -> success, with the gap between
                             attempts DOUBLING (15s, then 30s).

  never_succeeds         ->  fails every time. retries=1 means 2 attempts, then
                             the task is terminally failed.

Trigger it manually from the UI and watch the Grid / Graph view.
"""

from datetime import datetime

# NOTE: import from dag_context, NOT dag_parser.dynamic.operators. The parser
# sandbox tolerates the latter, but a Python task re-imports this module on the
# worker at execution time, where only dag_context exists.
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def succeeds_on_third_attempt(**context):
    attempt = context["try_number"]
    print(f"--- attempt number {attempt} ---", flush=True)
    if attempt < 3:
        raise RuntimeError(f"deliberate failure on attempt {attempt}")
    print("succeeded on attempt 3", flush=True)
    return {"attempts_taken": attempt}


def always_fails(**context):
    attempt = context["try_number"]
    print(f"--- attempt number {attempt}, this one never succeeds ---", flush=True)
    raise RuntimeError(f"deliberate failure on attempt {attempt}")


with DAG(
    dag_id="p3_13_retry_policy",
    description="Feature 13 - retries, retry_delay_seconds and exponential backoff",
    schedule=None,                 # manual trigger only, so the demo runs on cue
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["session", "part-3", "feature-13"],
    # Set the retry policy ONCE for the whole DAG...
    default_args={
        "retries": 1,
        "retry_delay_seconds": 15,
        "retry_exponential_backoff": True,
        "max_retry_delay_seconds": 60,
    },
) as dag:

    # ...and override just the retry COUNT here. Everything else is inherited:
    # still exponential, still 15s base, still capped at 60s.
    recovers = PythonOperator(
        task_id="succeeds_on_attempt_3",
        python_callable=succeeds_on_third_attempt,
        provide_context=True,
        retries=3,                 # task-level value beats the DAG default
    )

    # Inherits retries=1 from default_args -> 2 attempts total, then failed.
    exhausts = PythonOperator(
        task_id="never_succeeds",
        python_callable=always_fails,
        provide_context=True,
    )
