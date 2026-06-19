"""
TEST 1: Custom Timetable - last_day_of_month (direct timetable param)
start_date = May 1 so next run = May 31 (already past) -> run created immediately.
Expected: Run created with execution_date = 2026-05-31 00:00:00
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_last_day_direct",
    schedule_interval=None,
    timetable="last_day_of_month",
    start_date=datetime(2026, 5, 1),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Last day of month run: {{ .DS }}'")
