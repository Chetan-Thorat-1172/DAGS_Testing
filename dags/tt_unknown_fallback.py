"""
TEST 4: Unknown timetable name
Expected: Scheduler logs a warning "unknown timetable, falling back to cron"
and treats schedule_interval as a cron expression (*/10 * * * *).
The run should still be created via the cron fallback path.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tt_unknown_fallback",
    schedule_interval="*/10 * * * *",
    timetable="nonexistent_timetable",
    start_date=datetime(2026, 6, 19),
    catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo 'Cron fallback run: {{ .DS }}'")
