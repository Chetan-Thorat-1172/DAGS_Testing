"""S3KeySensor live test — waiting -> timeout, soft_fail, poke vs reschedule.

Each sensor waits on a key that does NOT exist, with a short cumulative timeout,
to demonstrate the "condition not met -> keep waiting -> time out" behavior.
Nothing is created, so no cleanup is needed. Trigger manually.

Expected outcomes:
  - wait_reschedule_fail : up_for_reschedule pokes, then FAILED at ~20s.
  - wait_reschedule_skip : up_for_reschedule pokes, then SKIPPED at ~20s (soft_fail).
  - wait_poke_fail       : holds the slot, pokes every 5s, then FAILED at ~20s.

Connection: an "Amazon Web Services" connection named `aws_test`.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, S3KeySensor

BUCKET = "maestro-pi-s3-test"
MISSING = "maestro-s3keysensor/does-not-exist.txt"

with DAG(
    dag_id="aws_s3keysensor_timeout",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "s3", "sensor"],
) as dag:

    # reschedule mode, hard fail on timeout
    S3KeySensor(
        task_id="wait_reschedule_fail",
        bucket_key=MISSING, bucket_name=BUCKET,
        mode="reschedule", poke_interval=5, timeout=20, soft_fail=False,
    )

    # reschedule mode, soft_fail -> timeout becomes a skip
    S3KeySensor(
        task_id="wait_reschedule_skip",
        bucket_key=MISSING, bucket_name=BUCKET,
        mode="reschedule", poke_interval=5, timeout=20, soft_fail=True,
    )

    # poke mode, hard fail on timeout (holds the worker slot while waiting)
    S3KeySensor(
        task_id="wait_poke_fail",
        bucket_key=MISSING, bucket_name=BUCKET,
        mode="poke", poke_interval=5, timeout=20, soft_fail=False,
    )
