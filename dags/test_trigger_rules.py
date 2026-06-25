from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator
# test
with DAG(
    dag_id="test_trigger_rules",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 23),
    catchup=False,
) as dag:

    ok1 = BashOperator(
        task_id="ok1",
        bash_command="echo success1",
    )

    ok2 = BashOperator(
        task_id="ok2",
        bash_command="echo success2",
    )

    fail1 = BashOperator(
        task_id="fail1",
        bash_command="exit 1",
        retries=0,
    )

    t_all_success = BashOperator(
        task_id="t_all_success",
        bash_command="echo all_success fired",
        trigger_rule="all_success",
    )

    t_none_failed = BashOperator(
        task_id="t_none_failed",
        bash_command="echo none_failed fired",
        trigger_rule="none_failed",
    )

    t_one_success = BashOperator(
        task_id="t_one_success",
        bash_command="echo one_success fired",
        trigger_rule="one_success",
    )

    t_all_done = BashOperator(
        task_id="t_all_done",
        bash_command="echo all_done fired",
        trigger_rule="all_done",
    )

    t_always = BashOperator(
        task_id="t_always",
        bash_command="echo always fired",
        trigger_rule="always",
    )

    t_nfmos = BashOperator(
        task_id="t_nfmos",
        bash_command="echo nfmos fired",
        trigger_rule="none_failed_min_one_success",
    )

    t_one_failed = BashOperator(
        task_id="t_one_failed",
        bash_command="echo one_failed fired",
        trigger_rule="one_failed",
    )

    t_all_failed = BashOperator(
        task_id="t_all_failed",
        bash_command="echo all_failed fired",
        trigger_rule="all_failed",
    )

    t_one_done = BashOperator(
        task_id="t_one_done",
        bash_command="echo one_done fired",
        trigger_rule="one_done",
    )

    [ok1, ok2] >> t_all_success
    [ok1, ok2] >> t_none_failed
    [ok1, fail1] >> t_one_success
    [ok1, fail1] >> t_all_done
    fail1 >> t_always
    [ok1, ok2] >> t_nfmos
    [ok1, fail1] >> t_one_failed
    [ok1, fail1] >> t_all_failed
    [ok1, fail1] >> t_one_done
