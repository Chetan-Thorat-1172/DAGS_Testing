"""
Test DAG: Dataset Consumer ALL mode (Session 49 - Dataset-Driven Scheduling)
Triggered ONLY when BOTH datasets have new events since last successful run.

Validates "all" trigger_type semantics:
  - DatasetEvaluator checks ALL URIs
  - Only fires when HasDatasetEventsSince() returns true for every URI
  - Deduplication: won't fire if a run is already queued/running

Test: Trigger test_dataset_producer (both datasets) → this fires.
      Trigger test_dataset_partial_producer (one dataset) → this does NOT fire.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

SALES_DATASET = Dataset("test://sales_table")
SESSIONS_DATASET = Dataset("test://sessions_table")

with DAG(
    dag_id="test_dataset_consumer_all",
    schedule=[SALES_DATASET, SESSIONS_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Triggered when ALL datasets (sales + sessions) have new events",
    tags=["test", "dataset"],
) as dag:

    validate = BashOperator(
        task_id="validate_inputs",
        bash_command="echo 'Both datasets available — ALL mode satisfied'",
    )

    process = BashOperator(
        task_id="build_combined_view",
        bash_command="echo 'Building combined analytics view from both datasets'",
    )

    validate >> process
