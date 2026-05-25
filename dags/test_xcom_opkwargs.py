"""
Test DAG: Validates Bug #28 (XCom Auto-Push), Bug #47 (op_kwargs), Bug #53/#54 (XCom Pull/Push Python)

Flow:
  upstream_task (returns a value via op_kwargs) 
    → downstream_task (pulls XCom from upstream, uses op_kwargs)

Expected behavior:
  1. upstream_task receives x=42, y="hello" via op_kwargs, returns their combination
  2. XCom auto-push persists the return value to the xcom table (Bug #28)
  3. downstream_task pulls the upstream value via ti.xcom_pull() (Bug #53/#54)
  4. downstream_task receives multiplier=10 via op_kwargs (Bug #47)
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime


def upstream_func(x, y, **kwargs):
    """Receives op_kwargs: x=42, y='hello'. Returns a combined result."""
    result = f"{y}_{x}"
    print(f"upstream_func called with x={x}, y={y}")
    print(f"Returning: {result}")
    return result


def downstream_func(multiplier, **kwargs):
    """Pulls XCom from upstream and uses op_kwargs."""
    ti = kwargs.get("ti")
    upstream_value = ti.xcom_pull(task_ids="upstream_task", key="return_value")
    print(f"downstream_func called with multiplier={multiplier}")
    print(f"Pulled XCom from upstream_task: {upstream_value}")
    
    if upstream_value is None:
        raise ValueError("XCom pull returned None — Bug #53/#54 not working!")
    
    result = f"{upstream_value}_x{multiplier}"
    print(f"Final result: {result}")
    return result


with DAG(
    dag_id="test_xcom_opkwargs",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Tests XCom auto-push, op_kwargs, and xcom_pull/push",
) as dag:

    upstream_task = PythonOperator(
        task_id="upstream_task",
        python_callable=upstream_func,
        op_kwargs={"x": 42, "y": "hello"},
    )

    downstream_task = PythonOperator(
        task_id="downstream_task",
        python_callable=downstream_func,
        op_kwargs={"multiplier": 10},
    )

    upstream_task >> downstream_task
