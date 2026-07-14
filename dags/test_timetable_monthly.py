from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
)

with DAG(
    dag_id="test_timetable_monthly",
    timetable="last_day_of_month",
    start_date=datetime(2026, 6, 1),
    catchup=True,
) as dag:

    t1 = BashOperator(
        task_id="month_end_close",
        bash_command="echo processing month-end",
    )
