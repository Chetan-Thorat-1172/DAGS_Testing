from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    ExternalTaskSensor,
)

with DAG(
    dag_id="test_soft_fail",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    # Sensor waits for a DAG that doesn't exist -> will timeout
    sensor = ExternalTaskSensor(
        task_id="soft_sensor",
        external_dag_id="nonexistent_dag",
        external_task_id="nonexistent_task",
        poke_interval=5,
        timeout=15,
        mode="reschedule",
        soft_fail=True,
    )

    # Downstream with none_failed: should still run after soft skip
    after_sensor = BashOperator(
        task_id="after_sensor",
        bash_command="echo 'I run despite sensor skip'",
        trigger_rule="none_failed",
    )

    sensor >> after_sensor
