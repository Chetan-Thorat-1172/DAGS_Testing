"""TEST 3: @daily preset - creates runs once per day at midnight"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_daily",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 18, 0, 0),
    catchup=False,
) as dag:
    BashOperator(task_id="daily_task", bash_command="echo 'Daily run at {{ .DS }}'")
