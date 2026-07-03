from datetime import datetime, timedelta
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_task_concurrency",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2026, 7, 2),
    catchup=True,
    max_active_runs=4,
) as dag:
    t_limited = BashOperator(
        task_id="t_limited",
        bash_command="sleep 120",
        task_concurrency=2,
    )
