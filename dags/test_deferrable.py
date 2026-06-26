from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    BaseTrigger,
    TaskDeferred,
)


class TimeDeltaTrigger(BaseTrigger):
    def __init__(self, delta_seconds):
        self.delta_seconds = delta_seconds

    def serialize(self):
        return {
            "delta_seconds": self.delta_seconds,
        }


def start_and_defer(**kwargs):
    print("Starting task, will defer for 30 seconds...", flush=True)

    raise TaskDeferred(
        trigger=TimeDeltaTrigger(delta_seconds=30),
        method_name="execute_complete",
        timeout=120,
    )


def resume_after(**kwargs):
    print("Resumed after defer!", flush=True)
    return "completed_after_defer"


with DAG(
    dag_id="test_deferrable",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    t_defer = PythonOperator(
        task_id="t_defer",
        python_callable=start_and_defer,
    )
