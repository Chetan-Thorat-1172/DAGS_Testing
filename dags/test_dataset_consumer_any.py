"""
Test DAG: Dataset Consumer ANY mode (Session 49 - Dataset-Driven Scheduling)
Triggered when ANY single dataset has a new event since last successful run.

Validates "any" trigger_type semantics:
  - DatasetEvaluator short-circuits on first dataset with new events
  - Fires even if only one of multiple watched datasets is updated
  - Still respects deduplication (no stacking while running/queued)

Test: Trigger test_dataset_partial_producer (only sales) → this STILL fires.
      Uses dict-based schedule syntax: {"datasets": [...], "trigger_type": "any"}
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

SALES_DATASET = Dataset("test://sales_table")
SESSIONS_DATASET = Dataset("test://sessions_table")

with DAG(
    dag_id="test_dataset_consumer_any",
    schedule={"datasets": [SALES_DATASET, SESSIONS_DATASET], "trigger_type": "any"},
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Triggered when ANY watched dataset has a new event",
    tags=["test", "dataset"],
) as dag:

    detect = BashOperator(
        task_id="detect_change",
        bash_command="echo 'Data change detected — ANY mode triggered'",
    )

    react = BashOperator(
        task_id="invalidate_cache",
        bash_command="echo 'Cache invalidated, downstream refresh triggered'",
    )

    detect >> react
