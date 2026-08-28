"""StepFunctionStartExecutionOperator redrive-path live wiring check.

Redrive only applies to a FAILED/TIMED_OUT/ABORTED Standard execution. Our test
state machine always succeeds, so this redrives a SUCCEEDED execution on purpose:
AWS rejects it (ExecutionNotRedrivable), which proves the RedriveExecution call
path is wired and the operator surfaces the AWS error cleanly. Pass the execution
NAME via conf:
    {"conf": {"exec_name": "<name segment of a real execution ARN>"}}

Expected outcome: task `redrive_exec` FAILS with an AWS redrive error.
Connection: `aws_test`. Needs states:RedriveExecution.
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, StepFunctionStartExecutionOperator

with DAG(
    dag_id="aws_stepfunction_redrive",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"aws_conn_id": "aws_test", "retries": 0},
    tags=["aws", "stepfunction", "redrive"],
) as dag:

    redrive_exec = StepFunctionStartExecutionOperator(
        task_id="redrive_exec",
        state_machine_arn="arn:aws:states:us-east-1:942195279389:stateMachine:maestro-test-sm",
        name="{{ .Params.exec_name }}",
        is_redrive_execution=True,
    )
