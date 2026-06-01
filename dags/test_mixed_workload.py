"""
Test DAG: Mixed Workload (Remote + Local)
=========================================
Tests that remote/sensor tasks are NOT blocked by admission control
while local compute tasks ARE gated.

- 20 "http_check" tasks (classified as Remote -> always admitted)
- 10 "python_compute" tasks (classified as Local -> admission gated)

All 30 tasks are parallel. Expected behavior:
- All 20 HTTP tasks dispatch immediately (ClassRemote)
- Only ADMISSION_MAX_LOCAL_TASKS (20) python tasks run concurrently (ClassLocal)

Trigger manually via API: POST /api/dags/mixed_workload_test/trigger
"""

from dag_context import DAG, task

dag = DAG(
    dag_id="mixed_workload_test",
    schedule_interval="@once",
    description="Mixed workload: remote tasks unrestricted, local tasks admission-gated",
    start_date="2026-06-01T00:00:00",
)

# These will be classified as ClassRemote (operator contains "http")
@task(dag=dag, task_id="http_check_{i}", operator="http_operator")
def http_check(task_id, **kwargs):
    """Simulates a remote HTTP call (lightweight - just sleep)."""
    import time
    time.sleep(2)  # Simulate network latency
    return {"task_id": task_id, "status": "ok"}

# These will be classified as ClassLocal (operator is "python")
@task(dag=dag, task_id="python_compute_{i}", operator="python_operator")
def python_compute(task_id, **kwargs):
    """CPU-intensive local computation."""
    import time
    result = 0
    start = time.time()
    while time.time() - start < 8:
        for j in range(100000):
            result += j * j
        result = result % 1000000
    return {"task_id": task_id, "result": result}

# Create parallel tasks
for i in range(20):
    http_check.override(task_id=f"http_check_{i}")(dag=dag)

for i in range(10):
    python_compute.override(task_id=f"python_compute_{i}")(dag=dag)
