"""LambdaInvokeFunctionOperator live test.

Invokes a small echo Lambda (maestro-test-echo, us-east-1) three ways:
  - invoke_sync  : RequestResponse + log_type=Tail; verify asserts the echoed
                   payload came back via XCom.
  - invoke_async : Event (fire-and-forget); succeeds, pushes no XCom.
  - invoke_error : payload {"fail": true} makes the function raise, so the task
                   fails on the response FunctionError.

The invoke_error task is expected to FAIL (that is the point of the test), so the
overall DAG run ends 'failed' — check the per-task states.

Connection: an "Amazon Web Services" connection named `aws_test`.
Lambda: maestro-test-echo (us-east-1).
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, LambdaInvokeFunctionOperator

FUNCTION = "maestro-test-echo"


def verify(**kwargs):
    ti = kwargs["ti"]
    val = ti.xcom_pull(task_ids="invoke_sync", key="return_value", map_indexes=-1)
    # XCom values are JSON-encoded on the way in; decode until we reach the object.
    for _ in range(3):
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except (ValueError, TypeError):
                break
        else:
            break
    print(f"verify pulled: {val!r}", flush=True)
    assert isinstance(val, dict), f"expected dict response, got {type(val)}: {val!r}"
    assert val.get("echo", {}).get("hello") == "maestro", f"unexpected response: {val!r}"
    print("verify OK", flush=True)


with DAG(
    dag_id="aws_lambda_invoke",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "lambda"],
) as dag:

    invoke_sync = LambdaInvokeFunctionOperator(
        task_id="invoke_sync",
        function_name=FUNCTION,
        payload='{"hello": "maestro", "n": 42}',
        invocation_type="RequestResponse",
        log_type="Tail",
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    invoke_async = LambdaInvokeFunctionOperator(
        task_id="invoke_async",
        function_name=FUNCTION,
        payload='{"hello": "async"}',
        invocation_type="Event",
    )

    invoke_error = LambdaInvokeFunctionOperator(
        task_id="invoke_error",
        function_name=FUNCTION,
        payload='{"fail": true}',
        invocation_type="RequestResponse",
    )

    invoke_sync >> check
