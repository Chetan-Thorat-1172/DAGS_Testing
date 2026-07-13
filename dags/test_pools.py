from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_pools",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=True,
) as dag:
    t1 = BashOperator(
        task_id="t1",
        bash_command="sleep 60",
        pool="test_pool",
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="sleep 60",
        pool="test_pool",
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command="sleep 60",
        pool="test_pool",
    )

    t4 = BashOperator(
        task_id="t4",
        bash_command="sleep 60",
        pool="test_pool",
    )
