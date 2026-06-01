"""
Test DAG: Resource-Aware Scaling Load Test
==========================================
Generates 30 parallel CPU-heavy Python tasks to test admission control.
Each task does CPU-intensive work for ~10 seconds.
With ADMISSION_MAX_LOCAL_TASKS=20, only 20 should run concurrently;
the rest should be queued by the admission controller.

Trigger manually via API: POST /api/dags/resource_aware_load_test/trigger
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime
import time


def cpu_heavy_task(**context):
    """Simulates a CPU-heavy local compute task (~10s of computation)."""
    ti = context["ti"]
    print(f"Starting CPU-heavy task: {ti.task_id}")

    result = 0
    start = time.time()
    while time.time() - start < 10:  # Run for ~10 seconds
        for j in range(100000):
            result += j * j
        result = result % 1000000

    duration = time.time() - start
    print(f"Task {ti.task_id} complete: result={result}, duration={duration:.1f}s")
    return {"result": result, "duration": duration}


with DAG(
    dag_id="resource_aware_load_test",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Load test: 30 parallel CPU-heavy tasks to validate admission control",
) as dag:

    # Create 30 parallel CPU-heavy tasks (no dependencies between them)
    for i in range(30):
        PythonOperator(
            task_id=f"cpu_heavy_{i}",
            python_callable=cpu_heavy_task,
            trigger_rule="always",
        )
