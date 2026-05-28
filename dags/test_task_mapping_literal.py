"""
Test DAG: Task Mapping / .expand() — Literal List

Validates:
1. PythonOperator.expand() with a literal list creates N instances
2. Each mapped instance receives its correct sliced argument
3. Downstream task waits for ALL mapped instances to complete

Flow:
  get_items → process_item.expand(item=["alpha","beta","gamma"]) → summarize
"""
from datetime import datetime


def get_items():
    """Returns a list that will be used for expansion."""
    return ["alpha", "beta", "gamma"]


def process_item(item):
    """Each mapped instance receives one item from the list."""
    return f"processed_{item}"


def summarize(**context):
    """Receives aggregated results from all mapped instances."""
    return "all_done"


dag = DAG(
    dag_id="test_task_mapping_literal",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Tests .expand() with literal list",
)

with dag:
    get_items_task = PythonOperator(
        task_id="get_items",
        python_callable=get_items,
    )

    process_task = PythonOperator(
        task_id="process_item",
        python_callable=process_item,
    ).expand(item=["alpha", "beta", "gamma"])

    summarize_task = PythonOperator(
        task_id="summarize",
        python_callable=summarize,
        provide_context=True,
    )

    get_items_task >> process_task >> summarize_task
