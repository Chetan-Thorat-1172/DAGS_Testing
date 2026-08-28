"""StepFunctionExecutionSensor + StepFunctionGetExecutionOutputOperator live test.

Waits for an execution to reach a terminal state (execution_arn passed via trigger
conf), pushes its output under XCom "output", then fetches the output again with
the get-output operator. Trigger with:
    {"conf": {"execution_arn": "<a real execution ARN from aws_stepfunction_start>"}}

Connection: `aws_test`. Needs states:DescribeExecution.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    StepFunctionExecutionSensor,
    StepFunctionGetExecutionOutputOperator,
)


def verify_output(**kwargs):
    ti = kwargs["ti"]
    # get-output operator: raw JSON string of the execution output
    out = ti.xcom_pull(task_ids="get_output", key="return_value", map_indexes=-1)
    print(f"get_output return_value: {out!r}", flush=True)
    assert isinstance(out, str) and out != "", f"expected output json string, got {out!r}"
    data = json.loads(out)
    print(f"parsed output: {data!r}", flush=True)
    # sensor pushed the same output under key "output"
    sensed = ti.xcom_pull(task_ids="wait_exec", key="output", map_indexes=-1)
    print(f"sensor output xcom: {sensed!r}", flush=True)
    print("verify OK", flush=True)


with DAG(
    dag_id="aws_stepfunction_wait",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "stepfunction", "sensor"],
) as dag:

    wait_exec = StepFunctionExecutionSensor(
        task_id="wait_exec",
        execution_arn="{{ .Params.execution_arn }}",
        poke_interval=5,
        timeout=120,
        mode="poke",
    )

    get_output = StepFunctionGetExecutionOutputOperator(
        task_id="get_output",
        execution_arn="{{ .Params.execution_arn }}",
    )

    check = PythonOperator(task_id="verify", python_callable=verify_output)

    wait_exec >> get_output >> check
