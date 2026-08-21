"""AWS S3 operator live smoke test (production-grade).

Exercises all five S3 operators end-to-end against a real bucket under a
dedicated prefix, ASSERTS the read/list results (fails loudly on mismatch),
and always cleans up (cleanup runs on all_done so objects never leak even if
an upstream task fails). Trigger manually.

Connection: an "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    S3CreateObjectOperator,
    S3ReadObjectOperator,
    S3CopyObjectOperator,
    S3ListOperator,
    S3DeleteObjectsOperator,
)

BUCKET = "maestro-pi-s3-test"
PREFIX = "maestro-s3-test"
EXPECTED_TEXT = "Hello from Maestro-pi S3 live test"
EXPECTED_KEYS = {
    f"{PREFIX}/hello.txt",
    f"{PREFIX}/data.json.gz",
    f"{PREFIX}/copy/hello.txt",
}


def verify_results(**kwargs):
    """Assert the read content and the listed keys are exactly what we wrote.
    Raises (fails the task) on any mismatch so the check actually bites."""
    import json

    ti = kwargs["ti"]

    # map_indexes=-1 targets the unmapped upstream XCom row. Without it, an
    # unmapped puller defaults to "all" (mapped fan-in) and misses map_index=-1.
    # XCom values come back JSON-encoded, so json.loads to get native types.
    content = ti.xcom_pull(task_ids="read_object", key="return_value", map_indexes=-1)
    keys = ti.xcom_pull(task_ids="list_objects", key="return_value", map_indexes=-1)
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (ValueError, TypeError):
            pass
    if isinstance(keys, str):
        keys = json.loads(keys)

    print(f"verify: read_object content = {content!r}", flush=True)
    print(f"verify: list_objects keys   = {keys}", flush=True)

    assert content == EXPECTED_TEXT, f"read mismatch: got {content!r}, want {EXPECTED_TEXT!r}"

    got = set(keys)
    missing = EXPECTED_KEYS - got
    assert not missing, f"list missing keys: {sorted(missing)}; got {sorted(got)}"

    print("verify: OK — read content and listed keys match", flush=True)
    return "verified"


with DAG(
    dag_id="aws_s3_smoke",
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
        data=EXPECTED_TEXT,
        replace=True,
    )

    read = S3ReadObjectOperator(
        task_id="read_object",
        s3_bucket=BUCKET,
        s3_key=f"{PREFIX}/hello.txt",
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

    list_keys = S3ListOperator(
        task_id="list_objects",
        bucket=BUCKET,
        prefix=f"{PREFIX}/",
    )

    verify = PythonOperator(
        task_id="verify",
        python_callable=verify_results,
    )

    # all_done: always clean up, even if an upstream task (incl. verify) failed,
    # so test objects never leak into the bucket.
    cleanup = S3DeleteObjectsOperator(
        task_id="delete_objects",
        bucket=BUCKET,
        prefix=f"{PREFIX}/",
        trigger_rule="all_done",
    )

    create >> read >> create_gzip >> copy >> list_keys >> verify >> cleanup
