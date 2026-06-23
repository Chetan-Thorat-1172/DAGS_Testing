from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
    BranchPythonOperator,
)


def pick_branch(**context):
    return "path_a"


with DAG(
    dag_id="tr_test_skips",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=pick_branch,
    )

    path_a = BashOperator(
        task_id="path_a",
        bash_command="echo path_a",
    )

    path_b = BashOperator(
        task_id="path_b",
        bash_command="echo path_b",
    )

    path_c = BashOperator(
        task_id="path_c",
        bash_command="echo path_c",
    )

    fail_x = BashOperator(
        task_id="fail_x",
        bash_command="exit 1",
        retries=0,
    )

    fail_y = BashOperator(
        task_id="fail_y",
        bash_command="exit 1",
        retries=0,
    )

    t_all_success_skip = BashOperator(
        task_id="t_all_success_skip",
        bash_command="echo should_not_run",
        trigger_rule="all_success",
    )

    t_none_failed_skip = BashOperator(
        task_id="t_none_failed_skip",
        bash_command="echo none_failed tolerates skips",
        trigger_rule="none_failed",
    )

    t_all_skipped = BashOperator(
        task_id="t_all_skipped",
        bash_command="echo all_skipped fired",
        trigger_rule="all_skipped",
    )

    t_none_skipped_skip = BashOperator(
        task_id="t_none_skipped_skip",
        bash_command="echo should_not_run",
        trigger_rule="none_skipped",
    )

    t_all_failed_all = BashOperator(
        task_id="t_all_failed_all",
        bash_command="echo all_failed fired",
        trigger_rule="all_failed",
    )

    branch >> [path_a, path_b, path_c]

    [path_a, path_b] >> t_all_success_skip
    [path_a, path_b] >> t_none_failed_skip
    [path_a, path_b] >> t_none_skipped_skip

    [path_b, path_c] >> t_all_skipped

    [fail_x, fail_y] >> t_all_failed_all
