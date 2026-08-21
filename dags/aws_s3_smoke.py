"""AWS S3 operator live smoke test.

Exercises all five S3 operators end-to-end against a real bucket, under a
dedicated prefix, and cleans up after itself. Trigger manually.

Connection: create an "Amazon Web Services" connection named `aws_test`
(Access Key auth, region us-east-1) in the Maestro UI first.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    S3CreateObjectOperator,
    S3ReadObjectOperator,
    S3CopyObjectOperator,
    S3ListOperator,
    S3DeleteObjectsOperator,
)

BUCKET = "maestro-pi-s3-test"
PREFIX = "maestro-s3-test"

with DAG(
    dag_id="aws_s3_smoke",
    schedule=None,
    start_date=datetime(2026, 8, 21),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3"],
) as dag:

    # 1. Create a plain UTF-8 object.
    create = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/hello.txt",
        data="Hello from Maestro-pi S3 live test",
        replace=True,
    )

    # 2. Read it back into XCom (verify contents downstream).
    read = S3ReadObjectOperator(
        task_id="read_object",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/hello.txt",
    )

    # 3. Create a gzip-compressed object.
    create_gzip = S3CreateObjectOperator(
        task_id="create_gzip",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/data.json.gz",
        data='{"event": "s3_test", "ok": true}',
        compression="gzip",
        replace=True,
    )

    # 4. Copy the first object to a sub-prefix.
    copy = S3CopyObjectOperator(
        task_id="copy_object",
        source_bucket_name=BUCKET,
        source_bucket_key=f"{PREFIX}/hello.txt",
        dest_bucket_name=BUCKET,
        dest_bucket_key=f"{PREFIX}/copy/hello.txt",
    )

    # 5. List everything under the prefix (pushed to XCom as a JSON list).
    list_keys = S3ListOperator(
        task_id="list_objects",
        bucket=BUCKET,
        prefix=f"{PREFIX}/",
    )

    # 6. Clean up: delete everything under the prefix via a prefix scan.
    cleanup = S3DeleteObjectsOperator(
        task_id="delete_objects",
        bucket=BUCKET,
        prefix=f"{PREFIX}/",
    )

    create >> read >> create_gzip >> copy >> list_keys >> cleanup
