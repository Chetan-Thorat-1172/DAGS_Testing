"""Feature 15 - Cross-run dependencies (depends_on_past).

Normally every DAG run is independent. depends_on_past says: this task may not
start until THE SAME TASK in the previous run succeeded.

Demo:
  1. Trigger with conf {"fail": true}   -> run 1's load FAILS
  2. Trigger again with no conf         -> run 2's load is BLOCKED, it never
                                           even starts, though it would pass
  3. Mark run 1's load as success in the UI -> run 2 unblocks and proceeds
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def load(**context):
    conf = context.get("conf") or {}
    if conf.get("fail"):
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
) as dag:

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
        depends_on_past=True,     # <- the whole feature is this one line
    )

    publish_task = PythonOperator(task_id="publish", python_callable=publish)

    load_task >> publish_task
