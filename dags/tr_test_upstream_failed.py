from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="tr_test_upstream_failed",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    # Root failure
    root_fail = BashOperator(
        task_id="root_fail",
        bash_command="exit 1",
        retries=0,
    )

    # L1: direct downstream of failure with all_success (default)
    # Expected: upstream_failed
    l1_default = BashOperator(
        task_id="l1_default",
        bash_command="echo should_not_run",
    )

    # L2: downstream of upstream_failed, still all_success
    # Expected: upstream_failed (cascade)
    l2_cascade = BashOperator(
        task_id="l2_cascade",
        bash_command="echo should_not_run",
    )

    # L1: uses all_done — should succeed despite root_fail
    l1_all_done = BashOperator(
        task_id="l1_all_done",
        bash_command="echo all_done ignores failure",
        trigger_rule="all_done",
    )

    # L1: uses always — should succeed despite root_fail
    l1_always = BashOperator(
        task_id="l1_always",
        bash_command="echo always ignores failure",
        trigger_rule="always",
    )

    root_fail >> l1_default
    l1_default >> l2_cascade

    root_fail >> l1_all_done
    root_fail >> l1_always
