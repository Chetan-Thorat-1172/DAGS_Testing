from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    TriggerDagRunOperator,
)

with DAG(
    dag_id="tdr_parent_wait",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:

    trigger = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="tdr_child",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    after = BashOperator(
        task_id="after_trigger",
        bash_command="echo 'child finished, parent continues'",
    )

    trigger >> after
