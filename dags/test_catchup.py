"""TEST 3: @daily preset - creates runs once per day at midnight"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_catchup",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 6, 22),
    catchup=True,
) as dag:
    BashOperator(task_id="daily_task", bash_command="echo 'Daily run at {{ .DS }}'")
    BashOperator(task_id="daily_task_2", bash_command="echo 'Daily run at {{ .DS }}'")

    daily_task >> daily_task_2
