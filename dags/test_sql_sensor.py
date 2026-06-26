from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, SqlSensor


with DAG(
    dag_id="test_sql_sensor",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    wait_for_data = SqlSensor(
        task_id="wait_for_flag",
        sql="SELECT 1 FROM variable WHERE key = 'sensor_test_fla'",
        poke_interval=10,
        timeout=120,
        mode="poke",
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'SqlSensor condition met!'",
    )

    wait_for_data >> done
