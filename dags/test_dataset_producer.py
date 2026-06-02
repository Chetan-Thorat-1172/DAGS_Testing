"""
Test DAG: Dataset Producer (Session 49 - Dataset-Driven Scheduling)
Produces a dataset event when its task completes successfully.
The consumer DAG (test_dataset_consumer) should auto-trigger. v2
"""
from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset
from datetime import datetime

with DAG(
    dag_id="test_dataset_producer",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Produces dataset event to trigger consumer DAG",
    tags=["test", "dataset"],
) as dag:

    produce_data = BashOperator(
        task_id="produce_data",
        bash_command="echo 'Producing data for test://sales_table'",
        outlets=[Dataset("test://sales_table")],
    )
