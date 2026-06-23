from datetime import datetime
    from dag_parser.dynamic.dag_context import DAG, BashOperator

    with DAG(
        dag_id="tr_test_rules",
        schedule_interval="@once",
        start_date=datetime(2026, 6, 23),
        catchup=False,
    ) as dag:

        # === Upstream tasks (mix of success and failure) ===
        ok1 = BashOperator(task_id="ok1", bash_command="echo success1")
        ok2 = BashOperator(task_id="ok2", bash_command="echo success2")
        fail1 = BashOperator(task_id="fail1", bash_command="exit 1")

        # === Trigger rule tests (each has different upstream combo) ===

        # all_success: fires only if ALL upstreams succeed
        # upstream: ok1, ok2 → should SUCCEED
        t_all_success = BashOperator(
            task_id="t_all_success",
            bash_command="echo 'all_success fired'",
            trigger_rule="all_success",
        )
        ok1 >> t_all_success
        ok2 >> t_all_success

        # none_failed: fires if no upstream failed (skips OK)
        # upstream: ok1, ok2 → should SUCCEED
        t_none_failed = BashOperator(
            task_id="t_none_failed",
            bash_command="echo 'none_failed fired'",
            trigger_rule="none_failed",
        )
        ok1 >> t_none_failed
        ok2 >> t_none_failed

        # one_success: fires if at least 1 upstream succeeded
        # upstream: ok1, fail1 → should SUCCEED (ok1 is enough)
        t_one_success = BashOperator(
            task_id="t_one_success",
            bash_command="echo 'one_success fired'",
            trigger_rule="one_success",
        )
        ok1 >> t_one_success
        fail1 >> t_one_success

        # all_done: fires when all upstreams are terminal (any state)
        # upstream: ok1, fail1 → should SUCCEED (both are terminal)
        t_all_done = BashOperator(
            task_id="t_all_done",
            bash_command="echo 'all_done fired'",
            trigger_rule="all_done",
        )
        ok1 >> t_all_done
        fail1 >> t_all_done

        # always: fires immediately regardless of upstream state
        # upstream: fail1 → should SUCCEED anyway
        t_always = BashOperator(
            task_id="t_always",
            bash_command="echo 'always fired'",
            trigger_rule="always",
        )
        fail1 >> t_always

        # none_failed_min_one_success: all done + no failures + at least 1 success
        # upstream: ok1, ok2 → should SUCCEED
        t_nfmos = BashOperator(
            task_id="t_nfmos",
            bash_command="echo 'none_failed_min_one_success fired'",
            trigger_rule="none_failed_min_one_success",
        )
        ok1 >> t_nfmos
        ok2 >> t_nfmos

        # one_failed: fires if at least 1 upstream failed
        # upstream: ok1, fail1 → should SUCCEED (fail1 failed)
        t_one_failed = BashOperator(
            task_id="t_one_failed",
            bash_command="echo 'one_failed fired'",
            trigger_rule="one_failed",
        )
        ok1 >> t_one_failed
        fail1 >> t_one_failed

        # all_failed: fires only if ALL upstreams failed
        # upstream: ok1, fail1 → should be SKIPPED (ok1 succeeded)
        t_all_failed = BashOperator(
            task_id="t_all_failed",
            bash_command="echo 'all_failed fired'",
            trigger_rule="all_failed",
        )
        ok1 >> t_all_failed
        fail1 >> t_all_failed

        # one_done: fires if at least 1 upstream is terminal
        # upstream: ok1, fail1 → should SUCCEED (ok1 finishes first)
        t_one_done = BashOperator(
            task_id="t_one_done",
            bash_command="echo 'one_done fired'",
            trigger_rule="one_done",
        )
        ok1 >> t_one_done
        fail1 >> t_one_done
