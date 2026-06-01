"""
Test DAG: Mixed Workload (Remote + Local)
=========================================
Tests that remote/sensor tasks are NOT blocked by admission control
while local compute tasks ARE gated.

- 20 "http_check" tasks (operator="HttpOperator" -> classified as Remote -> always admitted)
- 10 "python_compute" tasks (operator="PythonOperator" -> classified as Local -> admission gated)

All 30 tasks are parallel. Expected behavior:
- All 20 HTTP tasks dispatch immediately (ClassRemote)
- Python tasks gated by ADMISSION_MAX_LOCAL_TASKS

Trigger manually via API: POST /api/dags/mixed_workload_test/trigger
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator, HttpOperator
from datetime import datetime
import time


def http_check_task(**context):
    """Simulates a remote HTTP call (lightweight - just sleep)."""
    ti = context["ti"]
    print(f"HTTP check task: {ti.task_id} - simulating network call...")
    time.sleep(2)  # Simulate network latency
    print(f"HTTP check task: {ti.task_id} - done")
    return {"status": "ok"}


def python_compute_task(**context):
    """CPU-intensive local computation."""
    ti = context["ti"]
    print(f"Python compute task: {ti.task_id} - starting heavy work...")

    result = 0
    start = time.time()
    while time.time() - start < 8:
        for j in range(100000):
            result += j * j
        result = result % 1000000

    duration = time.time() - start
    print(f"Python compute task: {ti.task_id} - done in {duration:.1f}s")
    return {"result": result}


with DAG(
    dag_id="mixed_workload_test",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Mixed workload: remote tasks unrestricted, local tasks admission-gated",
) as dag:

    # HTTP tasks (will be classified as ClassRemote by executor_class.go)
    for i in range(20):
        HttpOperator(
            task_id=f"http_check_{i}",
            python_callable=http_check_task,
            trigger_rule="always",
        )

    # Python tasks (will be classified as ClassLocal by executor_class.go)
    for i in range(10):
        PythonOperator(
            task_id=f"python_compute_{i}",
            python_callable=python_compute_task,
            trigger_rule="always",
        )
