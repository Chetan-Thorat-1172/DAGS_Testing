"""AthenaSensor live test.

Waits for an Athena query (its query_execution_id passed in via trigger conf) to
reach a terminal state. Trigger with:
    {"conf": {"query_id": "<a real query_execution_id>"}}

Connection: an "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, AthenaSensor

with DAG(
    dag_id="aws_athena_sensor",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "athena", "sensor"],
) as dag:

    AthenaSensor(
        task_id="wait_query",
        query_execution_id="{{ .Params.query_id }}",
        mode="poke",
        poke_interval=5,
        timeout=60,
    )
