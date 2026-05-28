"""
Test DAG: HttpSensor in poke mode.
Waits for an HTTP endpoint to return 200, then runs a downstream task.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, HttpSensor, BashOperator

with DAG(
    dag_id="test_sensor_http",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    wait_for_api = HttpSensor(
        task_id="wait_for_api",
        endpoint="http://localhost:8080/health",
        method="GET",
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    process_data = BashOperator(
        task_id="process_data",
        bash_command="echo 'API is ready, processing data...'",
    )

    wait_for_api >> process_data
