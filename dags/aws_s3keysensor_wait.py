"""S3KeySensor live test — wait for a manually-uploaded file.

A single sensor waits (reschedule mode, poke every 10s, 10-minute timeout) for a
key that does NOT exist at trigger time. Upload the file by hand while it waits
and the sensor should flip from up_for_reschedule -> success on the next poke.

Upload target:
    s3://maestro-pi-s3-test/maestro-s3keysensor-wait/upload-me.txt

Connection: an "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, S3KeySensor

BUCKET = "maestro-pi-s3-test"
KEY = "maestro-s3keysensor-wait/upload-me.txt"

with DAG(
    dag_id="aws_s3keysensor_wait",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3", "sensor"],
) as dag:

    S3KeySensor(
        task_id="wait_for_upload",
        bucket_key=KEY,
        bucket_name=BUCKET,
        mode="reschedule",
        poke_interval=10,
        timeout=600,
    )
