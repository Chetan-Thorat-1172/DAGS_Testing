"""Feature 33 - Branching.

Everything so far ran a FIXED graph. A branch decides at RUNTIME which way to go.

    start -> choose_path -> full_load        -> join
                         -> incremental_load

choose_path RETURNS the task_id it wants. The paths it does not name are marked
skipped. join uses none_failed_min_one_success so a skipped sibling does not
block it.

Flip the "full_load" switch in the Trigger dialog to change which way it goes.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    Param,
    PythonOperator,
    BranchPythonOperator,
)


def choose_path(**context):
    # MUST return a task_id (or a list of them). Returning None fails the task.
    if context["params"].get("full_load"):
        return "full_load"
    return "incremental_load"


def full_load():
    print("reloading everything from scratch", flush=True)


def incremental_load():
    print("loading only what changed", flush=True)


def start():
    print("starting", flush=True)


def join():
    print("both paths lead here", flush=True)


with DAG(
    dag_id="06_branching",
    description="Feature 33 - BranchPythonOperator",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
    params={
        "full_load": Param(
            type="boolean",
            default=False,
            description="ON = full reload, OFF = incremental",
        ),
    },
) as dag:

    start_task = PythonOperator(task_id="start", python_callable=start)

    branch = BranchPythonOperator(
        task_id="choose_path",
        python_callable=choose_path,
        provide_context=True,
    )

    full = PythonOperator(task_id="full_load", python_callable=full_load)
    incremental = PythonOperator(task_id="incremental_load", python_callable=incremental_load)

    join_task = PythonOperator(
        task_id="join",
        python_callable=join,
        # WITHOUT this, the default all_success would see a 'skipped' parent
        # and block for ever. This is the single most common branching mistake.
        trigger_rule="none_failed_min_one_success",
    )

    # The SAME join, but left on the default all_success. Watch what happens
    # to it: one of its parents is skipped, so it can never be satisfied.
    join_default = PythonOperator(
        task_id="join_default_rule",
        python_callable=join,
        # trigger_rule not set -> all_success
    )

    start_task >> branch >> [full, incremental] >> join_task
    [full, incremental] >> join_default
