from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_setup_teardown_fail",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    setup = BashOperator(
        task_id="setup",
        bash_command="echo 'creating resources'",
        is_setup=True,
    )

    work = BashOperator(
        task_id="work",
        bash_command="exit 1",
        retries=0,
    )

    teardown = BashOperator(
        task_id="teardown",
        bash_command="echo 'cleanup despite failure'",
        is_teardown=True,
    )

    setup >> work >> teardown
