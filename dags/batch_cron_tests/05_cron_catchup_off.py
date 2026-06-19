"""TEST 5: catchup=False - creates only the LATEST missed run, skips intermediate"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="cron_catchup_off",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2026, 6, 14, 0, 0),  # 5 days ago
    catchup=False,
) as dag:
    BashOperator(task_id="latest_only", bash_command="echo 'Only latest run at {{ .DS }}'")
