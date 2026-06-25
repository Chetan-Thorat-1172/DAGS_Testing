from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="test_timeout_kill",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:
    # This sleeps 300s but has 15s timeout → should be killed at 15s
    t1 = BashOperator(
        task_id="slow_task",
        bash_command="echo 'starting...' && sleep 300 && echo 'should never print'",
        execution_timeout=15,
    )

    # This runs after timeout failure — should be upstream_failed
    t2 = BashOperator(
        task_id="after_timeout",
        bash_command="echo 'should not run'",
    )

    t1 >> t2
