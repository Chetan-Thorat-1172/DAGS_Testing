"""TEST 2: @once - creates exactly ONE run at start_date, never again"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_once",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 19, 12, 0),
    catchup=False,
) as dag:
    BashOperator(task_id="once_task", bash_command="echo 'This runs once'")
