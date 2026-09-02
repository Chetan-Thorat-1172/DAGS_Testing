"""Feature 13 - Retry policy.

One task that fails its first two attempts and succeeds on the third.
Watch the Grid view: running -> up_for_retry -> running -> up_for_retry
-> running -> success, with the wait DOUBLING between attempts.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def flaky(**context):
    attempt = context["try_number"]
    print(f"attempt {attempt}", flush=True)
    if attempt < 3:
        raise RuntimeError(f"attempt {attempt} failed on purpose")
    print("succeeded", flush=True)


with DAG(
    dag_id="01_retry",
    description="Feature 13 - retries and exponential backoff",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["session"],
) as dag:

    PythonOperator(
        task_id="flaky_task",
        python_callable=flaky,
        provide_context=True,
        retries=3,                          # 3 retries = 4 attempts in total
        retry_delay_seconds=10,             # wait 10s...
        retry_exponential_backoff=True,     # ...then 20s, then 40s
        max_retry_delay_seconds=60,         # never wait longer than this
    )
