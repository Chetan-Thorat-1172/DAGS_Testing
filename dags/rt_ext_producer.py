"""
Regression (Cat-2 / L1-06): producer DAG whose task FAILS, used as the external
target for rt_ext_sensor's failed_states fail-fast test.

Trigger manually FIRST: POST /api/dag-runs {"dag_id":"rt_ext_producer"}
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def boom(**context):
    raise Exception("intentional producer failure for external failed_states regression")


with DAG(
    dag_id="rt_ext_producer",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Regression: external target DAG that fails (for failed_states test)",
    tags=["regression", "cat2", "external"],
) as dag:
    producer_task = PythonOperator(task_id="producer_task", python_callable=boom, retries=0)
