"""
Test DAG: SqlSensor in reschedule mode.
Waits for data to appear in a staging table before proceeding.
Releases worker slot between pokes.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, SqlSensor, BashOperator

with DAG(
    dag_id="test_sensor_sql",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    wait_for_data = SqlSensor(
        task_id="wait_for_data",
        sql="SELECT COUNT(*) FROM staging.raw_events WHERE load_date = '{{ .DS }}'",
        conn_id="snowflake_default",
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="echo 'Data available, transforming...'",
    )

    wait_for_data >> transform
