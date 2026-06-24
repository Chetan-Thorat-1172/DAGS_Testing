from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="ext_producer",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    produce = BashOperator(
        task_id="produce_data",
        bash_command="echo 'data ready'",
    )
