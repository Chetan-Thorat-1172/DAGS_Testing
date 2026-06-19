"""TEST 1: Basic Cron Scheduling - runs every 2 minutes, catchup=False"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_basic",
    schedule_interval="*/2 * * * *",
    start_date=datetime(2026, 6, 19),
    catchup=False,
) as dag:
    BashOperator(task_id="echo", bash_command="echo 'Basic cron test at {{ .TS }}'")
