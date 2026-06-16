"""
Regression (Cat-2 / L1-05 + L2-06): soft_fail sensor timeout => 'skipped',
and the skip propagates to an all_success downstream.

wait: ExternalTaskSensor for a non-existent external DAG, soft_fail=True,
short timeout. It never finds the target -> times out -> must end 'skipped'
(NOT 'failed'). downstream (all_success default) -> must end 'skipped'.

Trigger manually: POST /api/dag-runs {"dag_id":"rt_soft_fail_sensor"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, ExternalTaskSensor, PythonOperator


def ok(**context):
    return "ok"


with DAG(
    dag_id="rt_soft_fail_sensor",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: soft_fail sensor timeout -> skipped -> propagates to downstream",
    tags=["regression", "cat2", "soft_fail"],
) as dag:
    wait = ExternalTaskSensor(
        task_id="wait",
        external_dag_id="does_not_exist_dag",
        external_task_id="does_not_exist_task",
        allowed_states=["success"],
        poke_interval=2,
        timeout=6,
        mode="poke",
        soft_fail=True,
    )
    downstream = PythonOperator(task_id="downstream", python_callable=ok)

    wait >> downstream
