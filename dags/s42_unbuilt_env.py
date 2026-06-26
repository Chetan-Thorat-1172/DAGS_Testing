from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def should_not_run():
    # Negative path: this env name was never built/staged. The worker MUST fail
    # loud at venv resolution ("managed venv not found") rather than silently
    # falling back to the base interpreter. Expected: task state = failed.
    return "should-not-run"


with DAG(
    dag_id="s42_unbuilt_env",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_unbuilt",
        python_callable=should_not_run,
        venv="never_built_xyz",
    )
