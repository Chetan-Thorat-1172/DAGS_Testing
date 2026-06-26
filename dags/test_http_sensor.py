from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, HttpSensor


with DAG(
    dag_id="test_http_sensor",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

     HttpSensor(
        task_id="wait_for_api",
        endpoint="Postman-echo.com/get",
        method="GET",
        poke_interval=10,
        timeout=60,
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'HttpSensor condition met!'",
    )

    wait_for_api >> done
