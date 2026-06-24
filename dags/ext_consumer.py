from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    ExternalTaskSensor,
)

with DAG(
    dag_id="ext_consumer",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer",
        external_dag_id="ext_producer",
        external_task_id="produce_data",
        allowed_states=["success"],
        poke_interval=5,
        timeout=120,
        mode="reschedule",
    )

    consume = BashOperator(
        task_id="consume_data",
        bash_command="echo 'consuming data'",
    )

    wait_for_producer >> consume
