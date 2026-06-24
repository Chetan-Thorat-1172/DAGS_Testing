from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_clear_version",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="step1",
        bash_command="echo 'V1 step1'",
    )

    t2 = BashOperator(
        task_id="step2",
        bash_command="echo 'V1 step2'",
    )

    t1 >> t2
