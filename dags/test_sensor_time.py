"""
Test DAG: TimeSensor in reschedule mode.
Waits until 09:00 before running downstream tasks.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, TimeSensor, BashOperator

with DAG(
    dag_id="test_sensor_time",
    schedule_interval="0 6 * * *",  # Runs at 06:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    wait_until_9am = TimeSensor(
        task_id="wait_until_9am",
        target_time="09:00",
        poke_interval=60,
        timeout=14400,
        mode="reschedule",
    )

    run_report = BashOperator(
        task_id="run_report",
        bash_command="echo 'It is 9am, running report...'",
    )

    wait_until_9am >> run_report
