from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_versioning2",
    schedule_interval=None,
    start_date=datetime(2026, 7, 15),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="step_1",
        bash_command="echo step1",
    )

    t2 = BashOperator(
        task_id="step_2",
        bash_command="sleep 150",
    )

    t3 = BashOperator(
        task_id="finish",
        bash_command="echo finish",
    )

    t1 >> t2
