"""
Test DAG: TaskFlow API - Mixed pattern (@task with classic DAG context manager)

Tests that @task decorator works with the classic `with DAG(...) as dag:` pattern.
Also tests @task with explicit parameters (task_id, retries).

Flow:
  generate_numbers() -> compute_stats(numbers)

Expected:
  1. generate_numbers returns a list of numbers
  2. compute_stats receives the list via XCom auto-pull, computes sum/avg
"""

from datetime import datetime

try:
    from dag_parser.dynamic.dag_context import DAG, PythonOperator, task
except (ImportError, NameError):
    pass


@task(task_id="generate_numbers")
def gen_numbers():
    """Generate a list of numbers."""
    numbers = [10, 20, 30, 40, 50]
    print(f"Generated: {numbers}")
    return numbers


@task(task_id="compute_stats")
def calc_stats(numbers):
    """Compute basic stats on the input list."""
    total = sum(numbers)
    avg = total / len(numbers)
    result = {"sum": total, "avg": avg, "count": len(numbers)}
    print(f"Stats: {result}")
    return result


with DAG(
    dag_id="test_taskflow_mixed",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="TaskFlow @task with classic DAG context manager",
    tags=["test", "taskflow"],
) as my_dag:

    nums = gen_numbers()
    calc_stats(nums)
