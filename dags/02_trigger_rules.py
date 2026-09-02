"""Feature 14 - Trigger rules.

One upstream succeeds, one fails. Three downstream tasks watch BOTH of them,
and each reacts differently purely because of its trigger_rule:

    needs_all  (all_success, the default)  ->  never runs
    alert      (one_failed)                ->  runs
    cleanup    (all_done)                  ->  runs
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok():
    print("this one works", flush=True)


def boom():
    raise RuntimeError("this one fails on purpose")


def note(msg):
    def _run():
        print(msg, flush=True)
    return _run


with DAG(
    dag_id="02_trigger_rules",
    description="Feature 14 - when is a task allowed to start?",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["session"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=ok)
    transform = PythonOperator(task_id="transform", python_callable=boom)

    # all_success is the DEFAULT: every upstream must succeed.
    # transform failed, so this is never allowed to start.
    needs_all = PythonOperator(
        task_id="needs_all",
        python_callable=note("only runs if everything upstream succeeded"),
        trigger_rule="all_success",
    )

    # one_failed: at least one upstream failed -> run. The alerting pattern.
    alert = PythonOperator(
        task_id="alert",
        python_callable=note("something upstream broke - sending the alert"),
        trigger_rule="one_failed",
    )

    # all_done: every upstream finished, no matter HOW. The cleanup pattern.
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=note("tidying up regardless of what happened"),
        trigger_rule="all_done",
    )

    [extract, transform] >> needs_all
    [extract, transform] >> alert
    [extract, transform] >> cleanup
