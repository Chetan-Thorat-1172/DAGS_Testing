"""TEST 8: @hourly preset - creates runs once per hour"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_hourly",
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 22),
    catchup=False,
) as dag:
    BashOperator(task_id="hourly_task", bash_command="echo 'Hourly run at {{ .TS }}'")
