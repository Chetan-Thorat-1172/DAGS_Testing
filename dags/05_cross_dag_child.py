"""Feature 27 - the CHILD half. Triggered by 05_cross_dag_parent.

It can still be run on its own - which is the whole point of splitting a DAG.
"""

import time
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def do_work(**context):
    # Whatever the parent put in conf= arrives here.
    batch_date = context["params"].get("batch_date", "<nothing was passed>")
    print(f"child running for batch_date = {batch_date}", flush=True)
    # Deliberately slow, so the parent is visibly parked in 'waiting_for_child'.
    time.sleep(25)
    print("child done", flush=True)


with DAG(
    dag_id="05_cross_dag_child",
    description="Feature 27 - the child DAG",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
) as dag:

    PythonOperator(
        task_id="do_work",
        python_callable=do_work,
        provide_context=True,
    )
