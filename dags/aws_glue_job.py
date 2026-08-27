"""GlueJobOperator live test — create-if-missing + run.

Creates the Glue job `maestro-test-job` (if absent) from the S3 script and runs
it, waiting for completion; asserts the JobRunId came back via XCom.

Connection: `aws_test`. Needs iam:PassRole on maestro-glue-exec-role + glue:*.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, GlueJobOperator


def verify(**kwargs):
    ti = kwargs["ti"]
    rid = ti.xcom_pull(task_ids="run_job", key="return_value", map_indexes=-1)
    if isinstance(rid, str):
        try:
            rid = json.loads(rid)
        except (ValueError, TypeError):
            pass
    print(f"JobRunId: {rid!r}", flush=True)
    assert isinstance(rid, str) and len(rid) > 5, f"bad JobRunId: {rid!r}"
    print("verify OK", flush=True)


with DAG(
    dag_id="aws_glue_job",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "glue"],
) as dag:

    run_job = GlueJobOperator(
        task_id="run_job",
        job_name="maestro-test-job",
        script_location="s3://maestro-pi-s3-test/glue-scripts/hello_glue.py",
        iam_role_name="maestro-glue-exec-role",
        create_job_kwargs={"GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2},
        job_poll_interval=15,
        execution_timeout=900,
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    run_job >> check
