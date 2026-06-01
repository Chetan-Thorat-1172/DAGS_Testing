"""
Test DAG: Resource-Aware Scaling Load Test
==========================================
Generates 30 parallel CPU-heavy Python tasks to test admission control.
Each task does CPU-intensive work (matrix multiplication) for ~10 seconds.
With ADMISSION_MAX_LOCAL_TASKS=20, only 20 should run concurrently;
the rest should be queued by the admission controller.

Trigger manually via API: POST /api/dags/resource_aware_load_test/trigger
"""

from dag_context import DAG, task

dag = DAG(
    dag_id="resource_aware_load_test",
    schedule_interval="@once",
    description="Load test: 30 parallel CPU-heavy tasks to validate admission control",
    start_date="2026-06-01T00:00:00",
)

@task(dag=dag, task_id="cpu_heavy_{i}")
def cpu_heavy_task(task_id, **kwargs):
    """Simulates a CPU-heavy local compute task (~10s of matrix ops)."""
    import time
    import random

    # CPU-intensive work: nested loops with math
    result = 0
    start = time.time()
    while time.time() - start < 10:  # Run for ~10 seconds
        for j in range(100000):
            result += j * j
        result = result % 1000000

    return {"task_id": task_id, "result": result, "duration": time.time() - start}

# Create 30 parallel tasks (no dependencies between them)
for i in range(30):
    cpu_heavy_task.override(task_id=f"cpu_heavy_{i}")(dag=dag)
