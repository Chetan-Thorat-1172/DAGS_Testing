"""
Test DAG: TimeSensor in reschedule mode.
Waits until 15:59 before running downstream tasks.
Uses short poke_interval (30s) to demonstrate reschedule behavior.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, TimeSensor, BashOperator

with DAG(
    dag_id="test_sensor_time",
    schedule_interval="0 6 * * *",  # Runs at 06:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    wait_for_time = TimeSensor(
        task_id="wait_for_time",
        target_time="15:59",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    run_report = BashOperator(
        task_id="run_report",
        bash_command="echo 'Target time reached, running report...'",
    )

    wait_for_time >> run_report
