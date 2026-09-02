"""Feature 15 - Cross-run dependencies (depends_on_past).

Normally every DAG run is independent. depends_on_past says: this task may not
start until THE SAME TASK in the previous run succeeded.

Demo - all from the UI, no curl:
  1. Trigger, TICK the "fail" checkbox   -> run 1's load FAILS
  2. Trigger again, leave it UNTICKED    -> run 2's load is BLOCKED. It never
                                            starts, though it would have passed
  3. Mark run 1's load as Success        -> run 2 unblocks and completes
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, Param


def load(**context):
    # Values ticked in the trigger form arrive here.
    if context["params"].get("fail"):
        raise RuntimeError("this run was told to fail")
    print("loaded today's slice", flush=True)


def publish():
    print("published", flush=True)


with DAG(
    dag_id="03_cross_run",
    description="Feature 15 - depends_on_past",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
    # Gives the Trigger dialog a real checkbox - no curl needed.
    params={
        "fail": Param(
            type="boolean",
            default=False,
            description="Tick this to make the load task fail on purpose",
        ),
    },
) as dag:

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
        depends_on_past=True,     # <- the whole feature is this one line
    )

    publish_task = PythonOperator(task_id="publish", python_callable=publish)

    load_task >> publish_task
