"""
DST Timezone Test DAG

Tests that the scheduler correctly handles per-DAG timezones.
This DAG runs every minute in America/New_York timezone to verify:
1. Timezone is correctly loaded and applied
2. Wall-clock dedup works (no duplicate runs for same wall-clock time)
3. Execution dates are stored correctly

To verify DST handling in production, check:
- Logs for "skipping duplicate wall-clock candidate" during fall-back
- No duplicate DAG runs for the same wall-clock time
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def print_execution_info(**context):
    """Print execution date info for verification."""
    exec_date = context.get('execution_date')
    run_id = context.get('run_id', 'unknown')
    print(f"Run ID: {run_id}")
    print(f"Execution Date: {exec_date}")
    if exec_date:
        print(f"Execution Date ISO: {exec_date.isoformat()}")
        print(f"Timezone: {getattr(exec_date, 'tzinfo', 'N/A')}")
        if hasattr(exec_date, 'strftime'):
            print(f"UTC Offset: {exec_date.strftime('%z')}")
            print(f"Wall Clock: {exec_date.strftime('%Y-%m-%d %H:%M:%S')}")
    return f"Executed run_id={run_id}"


with DAG(
    dag_id='test_dst_timezone',
    description='Tests timezone handling and DST wall-clock dedup',
    # Run every minute to quickly verify scheduling works
    schedule_interval='* * * * *',
    start_date=datetime(2025, 1, 1),
    # Use America/New_York to test DST handling
    # Fall-back: Nov 2, 2025 (1:00-1:59 AM repeats)
    # Spring-forward: Mar 9, 2025 (2:00-2:59 AM doesn't exist)
    timezone='America/New_York',
    catchup=False,  # Don't backfill, just test current scheduling
    max_active_runs=3,
    tags=['test', 'dst', 'timezone'],
) as dag:
    
    task = PythonOperator(
        task_id='print_exec_info',
        python_callable=print_execution_info,
    )
