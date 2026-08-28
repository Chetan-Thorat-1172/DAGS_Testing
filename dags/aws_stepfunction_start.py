"""StepFunctionStartExecutionOperator live test.

Starts an execution of the Standard state machine `maestro-test-sm` with a JSON
input and asserts the execution ARN came back via XCom (return_value).

Connection: `aws_test`. Needs states:StartExecution.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, StepFunctionStartExecutionOperator


def verify(**kwargs):
    ti = kwargs["ti"]
    arn = ti.xcom_pull(task_ids="start_exec", key="return_value", map_indexes=-1)
    print(f"execution_arn: {arn!r}", flush=True)
    assert isinstance(arn, str) and ":execution:" in arn, f"bad execution arn: {arn!r}"
    print("verify OK", flush=True)


with DAG(
    dag_id="aws_stepfunction_start",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "stepfunction"],
) as dag:

    start_exec = StepFunctionStartExecutionOperator(
        task_id="start_exec",
        state_machine_arn="arn:aws:states:us-east-1:942195279389:stateMachine:maestro-test-sm",
        state_machine_input={"foo": "bar", "n": 1},
    )

    check = PythonOperator(task_id="verify", python_callable=verify)

    start_exec >> check
