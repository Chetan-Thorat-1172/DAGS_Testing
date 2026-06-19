"""
TEST 2: Custom Timetable - @last_day_of_month shorthand
Same as tt_last_day_direct but uses schedule_interval="@last_day_of_month".
Scheduler redirects this to the last_day_of_month timetable internally.
Expected: Run created with execution_date = 2026-05-31 00:00:00
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_last_day_shorthand",
    schedule_interval="@last_day_of_month",
    start_date=datetime(2026, 5, 1),
    catchup=True,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Last day shorthand run: {{ .DS }}'")
