"""SnsPublishOperator live test — FIFO topic.

Publishes to a FIFO SNS topic with message_group_id + message_deduplication_id
and asserts the response carries both a MessageId and a SequenceNumber (FIFO
only). Subject is intentionally omitted (SNS FIFO topics do not support it).

Connection: an "Amazon Web Services" connection named `aws_test`.
Topic: arn:aws:sns:us-east-1:942195279389:maestro-test-topic.fifo
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnsPublishOperator

TOPIC_ARN = "arn:aws:sns:us-east-1:942195279389:maestro-test-topic.fifo"


def verify(**kwargs):
    ti = kwargs["ti"]
    val = ti.xcom_pull(task_ids="publish_fifo", key="return_value", map_indexes=-1)
    for _ in range(3):
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except (ValueError, TypeError):
                break
        else:
            break
    print(f"verify pulled: {val!r}", flush=True)
    assert isinstance(val, dict), f"expected dict, got {type(val)}: {val!r}"
    assert val.get("MessageId"), f"no MessageId: {val!r}"
    assert val.get("SequenceNumber"), f"no SequenceNumber (FIFO): {val!r}"
    print("verify OK — MessageId:", val["MessageId"], "SequenceNumber:", val["SequenceNumber"], flush=True)


with DAG(
    dag_id="aws_sns_publish_fifo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "sns", "fifo"],
) as dag:

    publish_fifo = SnsPublishOperator(
        task_id="publish_fifo",
        target_arn=TOPIC_ARN,
        message="FIFO hello from Maestro-pi ({{ .DS }})",
        message_group_id="maestro-test",
        message_deduplication_id="maestro-{{ .DS }}",
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    publish_fifo >> check
