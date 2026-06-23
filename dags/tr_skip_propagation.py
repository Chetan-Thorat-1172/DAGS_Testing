from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    BranchPythonOperator,
)


def pick_a(**context):
    return "path_a"


with DAG(
    dag_id="tr_skip_propagation",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=pick_a,
    )

    path_a = BashOperator(
        task_id="path_a",
        bash_command="echo path_a",
    )

    path_b = BashOperator(
        task_id="path_b",
        bash_command="echo path_b",
    )

    # Multi-level skip cascade through path_b (skipped)
    l2_skip = BashOperator(
        task_id="l2_skip",
        bash_command="echo should_not_run",
    )

    l3_skip = BashOperator(
        task_id="l3_skip",
        bash_command="echo should_not_run",
    )

    # Cascade broken at L3 by none_failed
    l3_broken = BashOperator(
        task_id="l3_broken",
        bash_command="echo none_failed breaks cascade",
        trigger_rule="none_failed",
    )

    # After cascade is broken, downstream resumes normally
    l4_after_break = BashOperator(
        task_id="l4_after_break",
        bash_command="echo normal after break",
    )

    # Mixed branch: path_a (success) + path_b (skipped)
    # all_success sees 1 success + 1 skip → skipped
    mixed_all_success = BashOperator(
        task_id="mixed_all_success",
        bash_command="echo should_not_run",
        trigger_rule="all_success",
    )

    # none_failed sees 1 success + 1 skip → success
    mixed_none_failed = BashOperator(
        task_id="mixed_none_failed",
        bash_command="echo none_failed handles mixed",
        trigger_rule="none_failed",
    )

    branch >> [path_a, path_b]

    path_b >> l2_skip
    l2_skip >> l3_skip
    l2_skip >> l3_broken

    l3_broken >> l4_after_break

    [path_a, path_b] >> mixed_all_success
    [path_a, path_b] >> mixed_none_failed
