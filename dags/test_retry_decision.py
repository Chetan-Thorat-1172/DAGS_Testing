from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator


attempt_count = 0


def flaky_task(**kwargs):
    import os

    # Use a file to track attempts across executions
    flag = "/tmp/retry_test_flag"

    if os.path.exists(flag):
        print("Flag exists - succeeding!", flush=True)
        return "success_on_retry"
    else:
        raise Exception("Simulated failure - no flag file yet")


with DAG(
    dag_id="test_retry_decision",
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    # This task always fails (to test retry behavior)
    t_fail = PythonOperator(
        task_id="t_always_fail",
        python_callable=flaky_task,
        retries=2,
        retry_delay_seconds=10,
    )

    t_done = BashOperator(
        task_id="t_done",
        bash_command="echo 'downstream ran'",
    )

    t_fail >> t_done
