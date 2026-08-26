"""AthenaOperator live test — table-less SELECT.

Runs `SELECT 1` on Athena (needs no table/database, only a results location),
waits for it to finish, and asserts the query_execution_id came back via XCom.

Connection: an "Amazon Web Services" connection named `aws_test`.
Results: s3://maestro-pi-s3-test/athena-results/
IAM for aws_test: athena:StartQueryExecution/GetQueryExecution/StopQueryExecution
  + s3 (Get/Put/List/GetBucketLocation) on maestro-pi-s3-test.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, AthenaOperator

OUTPUT = "s3://maestro-pi-s3-test/athena-results/"


def verify(**kwargs):
    ti = kwargs["ti"]
    qid = ti.xcom_pull(task_ids="run_query", key="return_value", map_indexes=-1)
    if isinstance(qid, str):
        try:
            qid = json.loads(qid)
        except (ValueError, TypeError):
            pass
    print(f"query_execution_id: {qid!r}", flush=True)
    assert isinstance(qid, str) and len(qid) > 10, f"bad query_execution_id: {qid!r}"
    print("verify OK", flush=True)


with DAG(
    dag_id="aws_athena_query",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "athena"],
) as dag:

    run_query = AthenaOperator(
        task_id="run_query",
        query="SELECT 1",
        output_location=OUTPUT,
        sleep_time=5,
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    run_query >> check
