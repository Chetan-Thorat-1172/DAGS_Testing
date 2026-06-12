"""
DAG Versioning Test DAG
=======================
Tests the DAG versioning feature with 4 sequential tasks.

Test Scenarios:
1. Trigger DAG -> Run should capture dag_hash at creation time
2. Modify DAG while running -> Running tasks should use original version
3. Clear completed task -> Cleared task should use NEW version

VERSION: 1.0.0 - Initial version
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator, PythonOperator

# ========================================
# VERSION MARKER - Change this to test versioning
# ========================================
DAG_VERSION = "v2.0.0"
TASK_MESSAGE = "Hello from version 2.0.0 - UPDATED!"


def print_version_info():
    """Print version info to verify which version is running."""
    import time
    print("=" * 50)
    print(f"DAG VERSION: {DAG_VERSION}")
    print(f"TASK MESSAGE: {TASK_MESSAGE}")
    print("=" * 50)
    time.sleep(5)
    return f"Completed with {DAG_VERSION}"


def slow_task():
    """A slow task that gives time to modify the DAG mid-flight."""
    import time
    print(f"Starting slow task - DAG_VERSION: {DAG_VERSION}")
    for i in range(10):
        print(f"Progress: {i+1}/10 - Running {DAG_VERSION}")
        time.sleep(2)  # 20 seconds total
    print(f"Slow task completed - {DAG_VERSION}")
    return "slow_task_done"


with DAG(
    dag_id="test_dag_versioning",
    description=f"DAG Versioning Test - {DAG_VERSION}",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "versioning"],
) as dag:

    # Task 1: Quick start task - prints version
    task_start = PythonOperator(
        task_id="task_start",
        python_callable=print_version_info,
    )

    # Task 2: Slow middle task - gives time to modify DAG
    task_slow = PythonOperator(
        task_id="task_slow",
        python_callable=slow_task,
    )

    # Task 3: Echo the version via bash
    task_echo = BashOperator(
        task_id="task_echo_version",
        bash_command=f'echo "Final task running: {DAG_VERSION} - {TASK_MESSAGE}"',
    )

    # Task 4: End task
    task_end = BashOperator(
        task_id="task_end",
        bash_command=f'echo "DAG completed successfully - {DAG_VERSION}"',
    )

    # Linear dependency chain
    task_start >> task_slow >> task_echo >> task_end
