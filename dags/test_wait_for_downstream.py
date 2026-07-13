from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_wait_for_downstream",
    schedule_interval=None,
    start_date=datetime(2026, 7, 5),
    catchup=True,
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo extract",
        wait_for_downstream=True,
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="echo transform",
    )

    load = BashOperator(
        task_id="load",
        bash_command="echo load",
    )

    extract >> transform >> load
