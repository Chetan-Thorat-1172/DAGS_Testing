from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    ExternalTaskSensor,
)

with DAG(
    dag_id="test_hard_fail",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    # Same sensor, but soft_fail=False (default)
    sensor = ExternalTaskSensor(
        task_id="hard_sensor",
        external_dag_id="nonexistent_dag",
        external_task_id="nonexistent_task",
        poke_interval=5,
        timeout=15,
        mode="reschedule",
        soft_fail=False,
    )

    # Downstream with none_failed: should not run
    after_sensor = BashOperator(
        task_id="after_sensor",
        bash_command="echo 'I should not run'",
        trigger_rule="none_failed",
    )

    sensor >> after_sensor
