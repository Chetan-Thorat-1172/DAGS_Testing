"""
Regression (Cat-2 / L1-00): the 'one_done' trigger rule (newly implemented).

upstreams: ok_task (success) + fail_task (failed, retries=0).
done_task (one_done) must RUN because >=1 upstream reached a terminal state.

Trigger manually: POST /api/dag-runs {"dag_id":"rt_trigger_one_done"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def ok(**context):
    return "ok"


def boom(**context):
    raise Exception("intentional failure for one_done regression")


with DAG(
    dag_id="rt_trigger_one_done",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: one_done runs when any upstream is done (success or failed)",
    tags=["regression", "cat2", "one_done"],
) as dag:
    ok_task = PythonOperator(task_id="ok_task", python_callable=ok)
    fail_task = PythonOperator(task_id="fail_task", python_callable=boom, retries=0)
    done_task = PythonOperator(task_id="done_task", python_callable=ok, trigger_rule="one_done")

    [ok_task, fail_task] >> done_task
