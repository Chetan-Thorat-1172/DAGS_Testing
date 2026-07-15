from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_versioning",
    schedule_interval=None,
    start_date=datetime(2026, 7, 14),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="step_1",
        bash_command="echo step1 && sleep 30",
    )

    t2 = BashOperator(
        task_id="step_2",
        bash_command="echo step2 && sleep 30",
    )

    t3 = BashOperator(
        task_id="step_3",
        bash_command="echo step3",
    )

    t4 = BashOperator(
        task_id="step_4_new",
        bash_command="echo NEW TASK",
    )

    t1 >> t2 >> t3 >> t4
