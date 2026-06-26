from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    BaseTrigger,
    TaskDeferred,
)


class DateTimeTrigger(BaseTrigger):
    def __init__(self, moment):
        self.moment = moment

    def serialize(self):
        return {
            "moment": self.moment,
        }


def start_and_defer(**kwargs):
    print("Deferring until target datetime...", flush=True)

    raise TaskDeferred(
        trigger=DateTimeTrigger(
            moment="2026-06-26T10:07:00Z",  # Replace with the desired UTC datetime
        ),
        method_name="execute_complete",
        timeout=300,
    )


def execute_complete(context=None, event=None, **kwargs):
    """
    Module-level resume function called when the DateTimeTrigger fires.
    """
    print("DateTime trigger fired!", flush=True)
    print(f"Context: {context}", flush=True)
    print(f"Event: {event}", flush=True)

    return "datetime_trigger_completed"


with DAG(
    dag_id="test_datetime_trigger",
    schedule_interval=None,
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:

    t_defer = PythonOperator(
        task_id="t_defer",
        python_callable=start_and_defer,
    )
