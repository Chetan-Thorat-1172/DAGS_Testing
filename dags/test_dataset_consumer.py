"""
Test DAG: Dataset Consumer (Session 49 - Dataset-Driven Scheduling)
Auto-triggered when the producer DAG emits a dataset event for test://sales_table.
No cron schedule - only runs when dataset is updated. v2
"""
from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset
from datetime import datetime

with DAG(
    dag_id="test_dataset_consumer",
    schedule=[Dataset("test://sales_table")],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Auto-triggered by dataset event from producer",
    tags=["test", "dataset"],
) as dag:

    consume_data = BashOperator(
        task_id="consume_data",
        bash_command="echo 'Consumer triggered! Dataset test://sales_table was updated.'",
    )
