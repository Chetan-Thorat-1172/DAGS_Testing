"""TEST 6: max_active_runs - won't exceed 2 concurrent runs"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_max_active",
    schedule_interval="* * * * *",  # Every minute
    start_date=datetime(2026, 6, 19, 0, 0),
    catchup=False,
    max_active_runs=2,
) as dag:
    # Long-running task to keep runs in "running" state
    BashOperator(task_id="slow_task", bash_command="sleep 120 && echo 'Done'")
