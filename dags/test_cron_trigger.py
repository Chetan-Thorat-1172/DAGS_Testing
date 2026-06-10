"""
test_cron_trigger.py — Validates that cron-scheduled DAGs fire automatically.

This DAG runs every minute with start_date in the past.
If the scheduler is working correctly, runs should be created automatically.
catchup=False ensures only the latest run fires (no backlog).

Expected: A new run every ~60 seconds in 'queued' → 'running' → 'success' state.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_cron_trigger",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 6, 9),
    catchup=False,
    timezone="Asia/Kolkata",
    description="Validates cron scheduling works - runs every minute",
    max_active_runs=2,
) as dag:

    task1 = BashOperator(
        task_id="cron_heartbeat",
        bash_command="echo 'Cron trigger fired at $(date)' && sleep 2",
    )
