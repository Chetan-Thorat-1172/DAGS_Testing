"""GlueJobSensor live test.

Waits for a Glue job run (job_name fixed, run_id passed via trigger conf) to reach
a terminal state. Trigger with:
    {"conf": {"run_id": "<a real JobRunId>"}}

Connection: `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, GlueJobSensor

with DAG(
    dag_id="aws_glue_job_sensor",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "glue", "sensor"],
) as dag:

    GlueJobSensor(
        task_id="wait_job",
        job_name="maestro-test-job",
        run_id="{{ .Params.run_id }}",
        mode="poke",
        poke_interval=10,
        timeout=180,
    )
