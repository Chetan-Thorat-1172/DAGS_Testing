"""
Test DAG: Dataset Chain — Middle (2 of 3)
Triggered by stage_1 dataset, produces stage_2 dataset.

Chain: source → [stage_1] → THIS → [stage_2] → sink
Validates that a dataset-triggered DAG can itself emit outlet events.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

STAGE_1_DATASET = Dataset("test://chain/stage_1")
STAGE_2_DATASET = Dataset("test://chain/stage_2")

with DAG(
    dag_id="test_dataset_chain_middle",
    schedule=[STAGE_1_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Chain middle: consumes stage_1, produces stage_2",
    tags=["test", "dataset", "chain"],
) as dag:

    transform = BashOperator(
        task_id="transform_and_enrich",
        bash_command="echo 'Transforming stage_1 → stage_2 (enrichment + dedup)'",
        outlets=[STAGE_2_DATASET],
    )
