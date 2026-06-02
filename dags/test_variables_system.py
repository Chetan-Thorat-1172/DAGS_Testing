"""
Test DAG: Variables System (Session 49)
Tests: {{ .Var.key }} template rendering in task params
"""
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator
from datetime import datetime

with DAG(
    dag_id="test_variables_system",
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    description="Tests PI-FLOW variables template rendering ({{ .Var.key }})",
    tags=["test", "variables"],
) as dag:

    # Task 1: BashOperator using variable in bash_command
    bash_with_var = BashOperator(
        task_id="bash_echo_var",
        bash_command="echo 'Environment is: {{ .Var.test_var }}' && echo 'DAG: {{ .DagID }}'",
    )

    # Task 2: PythonOperator using variable in op_kwargs
    def print_variable(**kwargs):
        var_value = kwargs.get("injected_var", "NOT_RESOLVED")
        print(f"Variable value received: {var_value}")
        if var_value == "NOT_RESOLVED" or var_value == "{{ .Var.test_var }}":
            raise Exception(f"Variable was not resolved! Got: {var_value}")
        return var_value

    python_with_var = PythonOperator(
        task_id="python_check_var",
        python_callable="print_variable",
        op_kwargs={"injected_var": "{{ .Var.test_var }}"},
    )

    bash_with_var >> python_with_var
