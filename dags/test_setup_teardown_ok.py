from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_setup_teardown_ok",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    setup = BashOperator(
        task_id="setup",
        bash_command="echo 'creating temp table'",
        is_setup=True,
    )

    work = BashOperator(
        task_id="work",
        bash_command="echo 'doing ETL'",
    )

    teardown = BashOperator(
        task_id="teardown",
        bash_command="echo 'dropping temp table'",
        is_teardown=True,
    )

    setup >> work >> teardown
