"""TEST 7: end_date cutoff - creates NO runs because end_date is in the past"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_ended",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 1, 0, 0),
    end_date=datetime(2026, 6, 10, 0, 0),  # Ended 9 days ago
    catchup=False,
) as dag:
    BashOperator(task_id="ended_task", bash_command="echo 'Should never run'")
