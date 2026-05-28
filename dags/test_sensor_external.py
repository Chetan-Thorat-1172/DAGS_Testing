"""
Test DAG: ExternalTaskSensor in reschedule mode.
Waits for the 'load_complete' task in 'data_ingestion' DAG to succeed.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, ExternalTaskSensor, BashOperator

with DAG(
    dag_id="test_sensor_external",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="data_ingestion",
        external_task_id="load_complete",
        allowed_states=["success"],
        poke_interval=120,
        timeout=10,
        mode="reschedule",
        soft_fail=True,
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command="echo 'Upstream complete, aggregating...'",
    )

    wait_for_upstream >> aggregate
