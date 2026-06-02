"""
Test DAG: Variables System (Session 49)
Tests: {{ .Var.key }} template rendering in task params
"""
from dag_parser.dynamic.dag_context import DAG, BashOperator
from datetime import datetime

with DAG(
    dag_id="test_variables_system",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Tests PI-FLOW variables template rendering",
    tags=["test", "variables"],
) as dag:

    # Task 1: BashOperator using variable in bash_command
    # {{ .Var.test_var }} should resolve to the value from the variables table
    bash_with_var = BashOperator(
        task_id="bash_echo_var",
        bash_command="echo 'Variable value: {{ .Var.test_var }}' && echo 'DagID: {{ .DagID }}'",
    )

    # Task 2: Verify the variable was actually resolved (not left as template literal)
    verify_var = BashOperator(
        task_id="verify_resolved",
        bash_command="VAL='{{ .Var.test_var }}' && if [ \"$VAL\" = '{{ .Var.test_var }}' ]; then echo 'FAIL: variable not resolved'; exit 1; fi && echo 'PASS: resolved to '$VAL",
    )

    bash_with_var >> verify_var
