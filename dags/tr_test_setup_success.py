from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="tr_test_setup_success",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    setup_ok = BashOperator(
        task_id="setup_ok",
        bash_command="echo setup succeeded",
        is_setup=True,
    )

    work_task = BashOperator(
        task_id="work_task",
        bash_command="echo work done",
    )

    t_setup_ok = BashOperator(
        task_id="t_setup_ok",
        bash_command="echo all_done_setup_success fired",
        trigger_rule="all_done_setup_success",
    )

    setup_fail = BashOperator(
        task_id="setup_fail",
        bash_command="exit 1",
        is_setup=True,
        retries=0,
    )

    work_task2 = BashOperator(
        task_id="work_task2",
        bash_command="echo work done 2",
    )

    t_setup_fail = BashOperator(
        task_id="t_setup_fail",
        bash_command="echo should_not_run",
        trigger_rule="all_done_setup_success",
    )

    [setup_ok, work_task] >> t_setup_ok
    [setup_fail, work_task2] >> t_setup_fail
