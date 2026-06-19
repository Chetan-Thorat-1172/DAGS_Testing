"""
TEST 1: Custom Timetable - last_day_of_month (direct timetable param)
Expected: Creates ONE run on the last day of the current month (June 30, 2026)
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_last_day_direct",
    schedule_interval=None,
    timetable="last_day_of_month",
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Last day of month run: {{ .DS }}'")
