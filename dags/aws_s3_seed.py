"""AWS S3 seed DAG — creates objects and leaves them in place (no cleanup).

Companion to aws_s3_smoke. Use this to visually confirm objects in the S3
console. Delete them afterwards by triggering aws_s3_smoke (its final task
prefix-deletes maestro-s3-test/) or via the console.

Connection: "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    S3CreateObjectOperator,
    S3CopyObjectOperator,
)

BUCKET = "maestro-pi-s3-test"
PREFIX = "maestro-s3-test"

with DAG(
    dag_id="aws_s3_seed",
    schedule=None,
    start_date=datetime(2026, 8, 21),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3"],
) as dag:

    create = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/hello.txt",
        data="Hello from Maestro-pi S3 live test",
        replace=True,
    )

    create_gzip = S3CreateObjectOperator(
        task_id="create_gzip",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/data.json.gz",
        data='{"event": "s3_test", "ok": true}',
        compression="gzip",
        replace=True,
    )

    copy = S3CopyObjectOperator(
        task_id="copy_object",
        source_bucket_name=BUCKET,
        source_bucket_key=f"{PREFIX}/hello.txt",
        dest_bucket_name=BUCKET,
        dest_bucket_key=f"{PREFIX}/copy/hello.txt",
    )

    create >> create_gzip >> copy
