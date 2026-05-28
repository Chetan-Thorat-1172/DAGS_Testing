"""
Test DAG: TaskFlow API - Basic @task/@dag pattern

Tests the TaskFlow API decorator pattern with automatic XCom passing.
Uses @dag to define the DAG and @task for each function-based task.
Dependencies are inferred from function call arguments (no >> needed).

Flow:
  extract() -> transform(data) -> load(results)

Expected:
  1. extract returns [1, 2, 3] (auto-pushed to XCom)
  2. transform receives [1, 2, 3] via XCom auto-pull, returns [2, 4, 6]
  3. load receives [2, 4, 6] via XCom auto-pull, prints and returns summary
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, task, dag


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     description="TaskFlow API basic test", tags=["test", "taskflow"])
def test_taskflow_basic():

    @task
    def extract():
        """Extract step - returns raw data."""
        data = [1, 2, 3, 4, 5]
        print(f"Extracted: {data}")
        return data

    @task
    def transform(data):
        """Transform step - doubles each value."""
        result = [x * 2 for x in data]
        print(f"Transformed: {data} -> {result}")
        return result

    @task
    def load(results):
        """Load step - summarizes the results."""
        total = sum(results)
        msg = f"Loaded {len(results)} items, total={total}"
        print(msg)
        return msg

    # TaskFlow dependency inference: passing return values as arguments
    data = extract()
    results = transform(data)
    load(results)


# Create the DAG by calling the decorated function
test_taskflow_basic()
