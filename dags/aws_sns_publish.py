"""SnsPublishOperator live test.

Publishes a message (with subject + typed message attributes) to a real SNS topic
and asserts the publish response (MessageId) came back via XCom.

Connection: an "Amazon Web Services" connection named `aws_test`.
Topic: arn:aws:sns:us-east-1:942195279389:maestro-test-topic
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnsPublishOperator

TOPIC_ARN = "arn:aws:sns:us-east-1:942195279389:maestro-test-topic"


def verify(**kwargs):
    ti = kwargs["ti"]
    val = ti.xcom_pull(task_ids="publish", key="return_value", map_indexes=-1)
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
    assert val.get("MessageId"), f"no MessageId in response: {val!r}"
    print("verify OK — MessageId:", val["MessageId"], flush=True)


with DAG(
    dag_id="aws_sns_publish",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "sns"],
) as dag:

    publish = SnsPublishOperator(
        task_id="publish",
        target_arn=TOPIC_ARN,
        message="Hello from Maestro-pi ({{ .DagID }} @ {{ .DS }})",
        subject="Maestro-pi SNS live test",
        message_attributes={"env": "prod", "priority": 1, "big": 9007199254740993},
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    publish >> check
