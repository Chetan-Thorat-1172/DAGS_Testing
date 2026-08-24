"""S3KeySensor live test — success paths (self-contained).

Seeds marker objects under a dedicated prefix, then exercises every S3KeySensor
success mode against a real bucket, and always cleans up (cleanup runs on
all_done so objects never leak even if a sensor fails). Trigger manually.

Connection: an "Amazon Web Services" connection named `aws_test`.
Bucket: maestro-pi-s3-test (us-east-1).
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    S3CreateObjectOperator,
    S3KeySensor,
    S3DeleteObjectsOperator,
)

BUCKET = "maestro-pi-s3-test"
PREFIX = "maestro-s3keysensor"

with DAG(
    dag_id="aws_s3keysensor_success",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3", "sensor"],
) as dag:

    # Seed three objects (chained so all exist before any sensor runs).
    seed_a = S3CreateObjectOperator(
        task_id="seed_a", s3_bucket=BUCKET, s3_key=f"{PREFIX}/a.txt",
        data="ready", replace=True,
    )
    seed_b = S3CreateObjectOperator(
        task_id="seed_b", s3_bucket=BUCKET, s3_key=f"{PREFIX}/b.txt",
        data="ready", replace=True,
    )
    seed_data = S3CreateObjectOperator(
        task_id="seed_data", s3_bucket=BUCKET, s3_key=f"{PREFIX}/data-01.json",
        data="{}", replace=True,
    )

    # Exact key, relative form (bucket_name + relative key), poke mode.
    wait_exact_relative = S3KeySensor(
        task_id="wait_exact_relative",
        bucket_key=f"{PREFIX}/a.txt", bucket_name=BUCKET,
        mode="poke", poke_interval=5, timeout=60,
    )
    # Exact key, full s3:// URL form (no bucket_name), reschedule mode.
    wait_exact_url = S3KeySensor(
        task_id="wait_exact_url",
        bucket_key=f"s3://{BUCKET}/{PREFIX}/b.txt",
        mode="reschedule", poke_interval=5, timeout=60,
    )
    # List of keys — succeeds only when ALL are present.
    wait_list = S3KeySensor(
        task_id="wait_list",
        bucket_key=[f"{PREFIX}/a.txt", f"{PREFIX}/b.txt"], bucket_name=BUCKET,
        mode="reschedule", poke_interval=5, timeout=60,
    )
    # Wildcard pattern ("*") — prefix-narrowed ListObjectsV2 scan.
    wait_wildcard = S3KeySensor(
        task_id="wait_wildcard",
        bucket_key=f"{PREFIX}/data-*.json", bucket_name=BUCKET,
        wildcard_match=True, mode="reschedule", poke_interval=5, timeout=60,
    )
    # Regex (start-anchored, re.match semantics) against bucket keys.
    wait_regex = S3KeySensor(
        task_id="wait_regex",
        bucket_key=rf"{PREFIX}/data-\d+\.json", bucket_name=BUCKET,
        use_regex=True, mode="reschedule", poke_interval=5, timeout=60,
    )

    cleanup = S3DeleteObjectsOperator(
        task_id="cleanup", bucket=BUCKET, prefix=f"{PREFIX}/",
        trigger_rule="all_done",
    )

    sensors = [
        wait_exact_relative, wait_exact_url, wait_list, wait_wildcard, wait_regex,
    ]
    seed_a >> seed_b >> seed_data >> sensors >> cleanup
