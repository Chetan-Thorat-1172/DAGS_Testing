"""SqsPublishOperator live test.

Sends a message (with delay_seconds + native SQS message attributes) to a real
SQS queue and asserts the send_message response (MessageId + MD5OfMessageBody)
came back via XCom.

Connection: an "Amazon Web Services" connection named `aws_test`.
Queue: https://sqs.us-east-1.amazonaws.com/942195279389/maestro-test-queue
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, SqsPublishOperator

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/942195279389/maestro-test-queue"


def verify(**kwargs):
    ti = kwargs["ti"]
    val = ti.xcom_pull(task_ids="send", key="return_value", map_indexes=-1)
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
    assert val.get("MD5OfMessageBody"), f"no MD5OfMessageBody: {val!r}"
    print("verify OK — MessageId:", val["MessageId"], flush=True)


with DAG(
    dag_id="aws_sqs_publish",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "sqs"],
) as dag:

    send = SqsPublishOperator(
        task_id="send",
        sqs_queue=QUEUE_URL,
        message_content="Hello from Maestro-pi ({{ .DagID }} @ {{ .DS }})",
        delay_seconds=0,
        message_attributes={
            "env": {"DataType": "String", "StringValue": "prod"},
            "priority": {"DataType": "Number", "StringValue": "1"},
        },
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    send >> check
