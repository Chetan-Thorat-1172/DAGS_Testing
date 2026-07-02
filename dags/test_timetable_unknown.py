from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
        dag_id="test_timetable_unknown",
        timetable="every_tuesday_afternoon",   # does not exist
        start_date=datetime(2026, 6, 1),
        catchup=False,
) as dag:
    BashOperator(task_id="run", bash_command="echo unknown timetable")
