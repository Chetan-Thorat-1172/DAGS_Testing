"""SqsSensor live test (self-contained).

Publishes a message to the queue, then SqsSensor receives it (deleting it on
reception) and pushes the batch to XCom under the key `messages`. `verify`
xcom_pulls with key="messages" — which fails if the named-XCom key is wrong.

Connection: an "Amazon Web Services" connection named `aws_test`.
Queue: https://sqs.us-east-1.amazonaws.com/942195279389/maestro-test-queue
IAM for aws_test: sqs:SendMessage, sqs:ReceiveMessage, sqs:DeleteMessage.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG, PythonOperator, SqsPublishOperator, SqsSensor,
)

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/942195279389/maestro-test-queue"


def verify(**kwargs):
    ti = kwargs["ti"]
    msgs = ti.xcom_pull(task_ids="wait_for_message", key="messages", map_indexes=-1)
    for _ in range(3):
        if isinstance(msgs, str):
            try:
                msgs = json.loads(msgs)
            except (ValueError, TypeError):
                break
        else:
            break
    print(f"verify pulled messages: {msgs!r}", flush=True)
    assert isinstance(msgs, list) and len(msgs) >= 1, f"expected >=1 message, got {msgs!r}"
    assert msgs[0].get("Body"), f"message missing Body: {msgs[0]!r}"
    print("verify OK — received", len(msgs), "message(s)", flush=True)


with DAG(
    dag_id="aws_sqs_sensor",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "sqs", "sensor"],
) as dag:

    publish = SqsPublishOperator(
        task_id="publish",
        sqs_queue=QUEUE_URL,
        message_content="hello-sensor {{ .DS }}",
    )

    wait_for_message = SqsSensor(
        task_id="wait_for_message",
        sqs_queue=QUEUE_URL,
        max_messages=10,
        wait_time_seconds=5,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    publish >> wait_for_message >> check
