"""
Test DAG: Dataset Chain — Source (1 of 3)
Manually triggered, produces stage_1 dataset to kick off the cascading chain.

Chain: source → [stage_1] → middle → [stage_2] → sink
All 3 fire from a single manual trigger of this DAG.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

STAGE_1_DATASET = Dataset("test://chain/stage_1")

with DAG(
    dag_id="test_dataset_chain_source",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Chain source: produces stage_1 dataset",
    tags=["test", "dataset", "chain"],
) as dag:

    produce = BashOperator(
        task_id="generate_events",
        bash_command="echo 'Generating raw events → stage_1'",
        outlets=[STAGE_1_DATASET],
    )
