"""
Test DAG: Jinja-style Template Rendering

Tests that Go text/template variables are rendered in operator params at runtime.
Uses {{ .DS }}, {{ .DagID }}, {{ .RunID }}, and macros like ds_add.

Flow:
  echo_date (BashOperator) -> report_info (PythonOperator)

Expected:
  1. echo_date runs bash_command with {{ .DS }} replaced by actual execution date
  2. report_info prints context showing template vars were resolved
"""

from datetime import datetime

try:
    from dag_parser.dynamic.dag_context import DAG, BashOperator, PythonOperator, task
except (ImportError, NameError):
    pass


def print_report(**kwargs):
    """Print the report info passed via op_kwargs (templates resolved by Go)."""
    print(f"DAG ID: {kwargs.get('dag_id', 'N/A')}")
    print(f"Run ID: {kwargs.get('run_id', 'N/A')}")
    print(f"DS: {kwargs.get('ds', 'N/A')}")
    print(f"DS yesterday: {kwargs.get('ds_yesterday', 'N/A')}")
    return {"dag_id": kwargs.get("dag_id"), "ds": kwargs.get("ds")}


with DAG(
    dag_id="test_jinja_templates",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Tests Go text/template rendering in operator params",
    tags=["test", "templates"],
) as dag:

    # BashOperator: {{ .DS }} and {{ .DagID }} should be rendered
    echo_date = BashOperator(
        task_id="echo_date",
        bash_command='echo "Date: {{ .DS }}" && echo "DAG: {{ .DagID }}" && echo "Run: {{ .RunID }}" && echo "Yesterday: {{ ds_add .DS -1 }}"',
    )

    # PythonOperator: template vars in op_kwargs should be rendered
    report_info = PythonOperator(
        task_id="report_info",
        python_callable=print_report,
        op_kwargs={
            "dag_id": "{{ .DagID }}",
            "run_id": "{{ .RunID }}",
            "ds": "{{ .DS }}",
            "ds_yesterday": "{{ ds_add .DS -1 }}",
        },
    )

    echo_date >> report_info
