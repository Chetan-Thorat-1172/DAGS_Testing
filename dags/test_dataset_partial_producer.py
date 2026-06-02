"""
Test DAG: Dataset Partial Producer (Session 49 - Dataset-Driven Scheduling)
Produces ONLY the sales dataset (not sessions).

Purpose: Validates ALL vs ANY distinction:
  - test_dataset_consumer_all should NOT fire (needs both datasets)
  - test_dataset_consumer_any SHOULD fire (only needs one)

Test procedure:
  1. Ensure both consumers have had at least one successful run (reset baseline)
  2. Trigger this DAG (produces only sales)
  3. Verify: consumer_any triggers, consumer_all does NOT
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

SALES_DATASET = Dataset("test://sales_table")

with DAG(
    dag_id="test_dataset_partial_producer",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Produces only sales dataset to test ALL vs ANY semantics",
    tags=["test", "dataset"],
) as dag:

    produce = BashOperator(
        task_id="produce_sales_only",
        bash_command="echo 'Producing partial update: sales only (not sessions)'",
        outlets=[SALES_DATASET],
    )
