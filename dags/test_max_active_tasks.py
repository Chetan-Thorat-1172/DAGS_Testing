from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator
# 
with DAG(
    dag_id="test_max_active_tasks",
    schedule_interval=None,
    start_date=datetime(2026, 7, 10),
    catchup=True,
    max_active_tasks=2,
) as dag:
    t1 = BashOperator(
        task_id="t1",
        bash_command="sleep 60",
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="sleep 60",
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command="sleep 60",
    )

    t4 = BashOperator(
        task_id="t4",
        bash_command="sleep 60",
    )
