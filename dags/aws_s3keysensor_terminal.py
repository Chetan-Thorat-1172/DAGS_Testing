"""S3KeySensor live test — terminal fail-fast on a missing bucket.

The sensor targets a bucket that does not exist. A missing bucket is a permanent
error, so the sensor should FAIL FAST (well before the 120s timeout) rather than
keep waiting. wildcard_match is used so the check goes through ListObjectsV2 (a
GET with a response body), which surfaces a parseable NoSuchBucket error code —
a plain HeadObject on a missing bucket returns a bodyless 404 that is
indistinguishable from "object absent". Trigger manually.

Expected outcome: wait_bad_bucket FAILS within a few seconds (not at 120s), with
a NoSuchBucket error in the task log.

Connection: an "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, S3KeySensor

with DAG(
    dag_id="aws_s3keysensor_terminal",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3", "sensor"],
) as dag:

    S3KeySensor(
        task_id="wait_bad_bucket",
        bucket_key="prefix/whatever-*.txt",
        bucket_name="maestro-pi-nonexistent-bucket-9z8q7w3",
        wildcard_match=True,
        mode="reschedule", poke_interval=5, timeout=120,
    )
