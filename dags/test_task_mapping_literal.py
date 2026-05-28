"""
Test DAG: Task Mapping / .expand() — Literal List
"""
from datetime import datetime

try:
    from pi_flow import DAG, PythonOperator, XComArg
except ImportError:
    pass  # Worker import — DAG/PythonOperator not needed for function execution


def get_items():
    """Returns a list that will be used for expansion."""
    return ["alpha", "beta", "gamma"]


def process_item(item):
    """Each mapped instance receives one item from the list."""
    return f"processed_{item}"


def summarize(**context):
    """Receives aggregated results from all mapped instances."""
    return "all_done"


try:
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
except NameError:
    pass  # Worker import context — DAG construction not needed
