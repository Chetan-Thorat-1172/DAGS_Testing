"""
TEST 3: Custom Timetable - business_days
start_date = yesterday (June 18) so next candidate = today June 19 (Thursday) at midnight.
Midnight is already past -> run created immediately.
Expected: Run created with execution_date = 2026-06-19 00:00:00 (today, a weekday)
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_business_days",
    schedule_interval=None,
    timetable="business_days",
    start_date=datetime(2026, 6, 18),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Business day run: {{ .DS }}'")
