from datetime import datetime, timedelta
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_max_active_runs",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2026, 7, 2),
    catchup=True,
    max_active_runs=2,
) as dag:
    t_slow = BashOperator(
        task_id="t_slow",
        bash_command="sleep 120",
    )
