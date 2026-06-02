"""
Test DAG: Scheduling Features (Session 48)
Tests: trigger rules, weight rules, wait_for_downstream, per-task SLA
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_scheduling_features",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Tests new scheduling features from Session 48",
) as dag:

    # --- Trigger Rule Tests ---
    always_succeed = BashOperator(
        task_id="always_succeed",
        bash_command="echo success",
    )

    always_fail = BashOperator(
        task_id="always_fail",
        bash_command="exit 1",
        retries=0,
    )

    # one_failed: fires when at least one upstream fails
    error_handler = BashOperator(
        task_id="error_handler",
        bash_command="echo error_handler_triggered",
        trigger_rule="one_failed",
    )

    # none_skipped: fires only when no upstream was skipped
    all_ran_check = BashOperator(
        task_id="all_ran_check",
        bash_command="echo all_ran",
        trigger_rule="none_skipped",
    )

    always_succeed >> error_handler
    always_fail >> error_handler
    always_succeed >> all_ran_check
    always_fail >> all_ran_check

    # --- Weight Rule Tests ---
    root_task = BashOperator(
        task_id="root_task",
        bash_command="echo root",
        priority_weight=1,
        weight_rule="downstream",
    )

    child_a = BashOperator(
        task_id="child_a",
        bash_command="echo a",
        priority_weight=5,
    )

    child_b = BashOperator(
        task_id="child_b",
        bash_command="echo b",
        priority_weight=5,
    )

    root_task >> child_a
    root_task >> child_b

    # --- wait_for_downstream Test ---
    gated_task = BashOperator(
        task_id="gated_task",
        bash_command="echo gated",
        wait_for_downstream=True,
    )

    gated_downstream = BashOperator(
        task_id="gated_downstream",
        bash_command="sleep 10",
    )

    gated_task >> gated_downstream

    # --- Per-Task SLA Test ---
    sla_task = BashOperator(
        task_id="sla_task",
        bash_command="sleep 120",
        sla=60,
    )
