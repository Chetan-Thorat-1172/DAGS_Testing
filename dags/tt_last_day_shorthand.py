"""
TEST 2: Custom Timetable - @last_day_of_month shorthand
Expected: schedule_interval="@last_day_of_month" is redirected by the scheduler
to the last_day_of_month timetable. Same behavior as tt_last_day_direct.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_last_day_shorthand",
    schedule_interval="@last_day_of_month",
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Last day shorthand run: {{ .DS }}'")
