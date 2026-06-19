"""
TEST 3: Custom Timetable - business_days
Expected: Creates runs only on Mon-Fri (weekdays). Skip Sat/Sun.
Start date is a past Monday to ensure a run is already due.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_business_days",
    schedule_interval=None,
    timetable="business_days",
    start_date=datetime(2026, 6, 19),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Business day run: {{ .DS }}'")
