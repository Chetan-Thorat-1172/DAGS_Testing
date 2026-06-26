from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, TimeSensor


with DAG(
    dag_id="test_time_sensor",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    wait_for_time = TimeSensor(
        task_id="wait_until",
        target_time="14:40",  # UTC (6 minutes after 08:58)
        poke_interval=10,
        timeout=520,  # 7 minutes
        mode="poke",
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'TimeSensor condition met!'",
    )

    wait_for_time >> done
