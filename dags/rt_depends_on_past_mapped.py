"""
Regression (Cat-2 / L3-01 + L1-02): depends_on_past + wait_for_downstream on a
mapped task. The flag must survive parsing onto every mapped instance, and the
cross-run gate must consider ALL map indices of the previous run.

run 1: mapped 'process' over 3 items (all succeed) -> 2nd run's 'process'
should be allowed (all prior indices succeeded).

Trigger manually twice: POST /api/dag-runs {"dag_id":"rt_depends_on_past_mapped"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def get_items(**context):
    return ["a", "b", "c"]


def process(item, **context):
    return f"processed_{item}"


def finalize(**context):
    return "done"


with DAG(
    dag_id="rt_depends_on_past_mapped",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: depends_on_past + wait_for_downstream on mapped task",
    tags=["regression", "cat2", "mapped", "dop"],
) as dag:
    seed = PythonOperator(task_id="seed", python_callable=get_items)
    proc = PythonOperator(
        task_id="process",
        python_callable=process,
        depends_on_past=True,
        wait_for_downstream=True,
    ).expand(item=["a", "b", "c"])
    fin = PythonOperator(task_id="finalize", python_callable=finalize)

    seed >> proc >> fin
