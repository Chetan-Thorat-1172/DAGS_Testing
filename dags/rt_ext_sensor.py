"""
Regression (Cat-2 / L1-06): ExternalTaskSensor with failed_states must FAIL FAST
when the external target entered a failed state, instead of polling until timeout.

Prereq: run rt_ext_producer first (its producer_task fails).
Then trigger: POST /api/dag-runs {"dag_id":"rt_ext_sensor"}

Expected: 'wait_for_producer' ends 'failed' QUICKLY (well under the 300s timeout)
because producer_task is in failed_states. downstream ends 'upstream_failed'.
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, ExternalTaskSensor, PythonOperator


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_ext_sensor",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: ExternalTaskSensor failed_states -> fail fast",
    tags=["regression", "cat2", "external"],
) as dag:
    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer",
        external_dag_id="rt_ext_producer",
        external_task_id="producer_task",
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=5,
        timeout=300,
        mode="poke",
        soft_fail=False,
    )
    downstream = PythonOperator(task_id="downstream", python_callable=ok)

    wait_for_producer >> downstream
