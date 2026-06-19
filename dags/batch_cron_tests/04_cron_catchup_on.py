"""TEST 4: catchup=True - creates MULTIPLE backfill runs from old start_date"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_catchup_on",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2026, 6, 14, 0, 0),  # 5 days ago
    catchup=True,
) as dag:
    BashOperator(task_id="backfill_task", bash_command="echo 'Backfill run at {{ .DS }}'")
