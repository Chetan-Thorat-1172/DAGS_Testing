"""
Test DAG: Dataset Chain — Sink (3 of 3)
Triggered by stage_2 dataset. Final consumer in the chain.

Chain: source → [stage_1] → middle → [stage_2] → THIS
Validates second-order cascading: event → trigger → event → trigger.
Full chain completes autonomously after single manual trigger of source.
"""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator, Dataset

STAGE_2_DATASET = Dataset("test://chain/stage_2")

with DAG(
    dag_id="test_dataset_chain_sink",
    schedule=[STAGE_2_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Chain sink: consumes stage_2, final load",
    tags=["test", "dataset", "chain"],
) as dag:

    load = BashOperator(
        task_id="load_warehouse",
        bash_command="echo 'Loading stage_2 into final warehouse table'",
    )

    verify = BashOperator(
        task_id="verify_chain_complete",
        bash_command="echo 'Chain complete: source → middle → sink all succeeded'",
    )

    load >> verify
