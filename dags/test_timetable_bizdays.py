from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
)

with DAG(
    dag_id="test_timetable_bizdays",
    timetable="business_days",
    start_date=datetime(2026, 7, 7),
    catchup=True,
) as dag:

    t1 = BashOperator(
        task_id="daily_report",
        bash_command="echo business day report",
    )
