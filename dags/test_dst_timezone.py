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
    exec_date = context.get('execution_date', 'N/A')
    run_id = context.get('run_id', 'unknown')
    
    print(f"Run ID: {run_id}")
    print(f"Execution Date: {exec_date}")
    print(f"Execution Date Type: {type(exec_date).__name__}")
    
    # execution_date may be a string in PI-FLOW context
    if isinstance(exec_date, str):
        print(f"Execution Date (raw string): {exec_date}")
        # Try to parse it for display
        try:
            from dateutil import parser as dt_parser
            parsed = dt_parser.parse(exec_date)
            print(f"Parsed Wall Clock: {parsed.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Parsed Timezone: {parsed.tzinfo}")
            print(f"Parsed UTC Offset: {parsed.strftime('%z')}")
        except Exception as e:
            print(f"Could not parse date: {e}")
    elif hasattr(exec_date, 'isoformat'):
        print(f"Execution Date ISO: {exec_date.isoformat()}")
        print(f"Timezone: {getattr(exec_date, 'tzinfo', 'N/A')}")
        if hasattr(exec_date, 'strftime'):
            print(f"UTC Offset: {exec_date.strftime('%z')}")
            print(f"Wall Clock: {exec_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return f"Executed run_id={run_id}"


with DAG(
    dag_id='test_dst_timezone',
    description='Tests timezone handling and DST wall-clock dedup',
    schedule_interval='* * * * *',
    start_date=datetime(2025, 1, 1),
    timezone='America/New_York',
    catchup=False,
    max_active_runs=3,
    tags=['test', 'dst', 'timezone'],
) as dag:
    
    task = PythonOperator(
        task_id='print_exec_info',
        python_callable=print_execution_info,
    )
