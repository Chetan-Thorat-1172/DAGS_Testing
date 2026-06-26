from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def use_named_env():
    # Back-compat path: reference a pre-built env by NAME (the S41 cowsay env
    # auto_19b97ee6bdb132eb), NOT requirements=[...]. Proves the venv= API still
    # resolves on the new image (go-core:v20260626-py12).
    import sys
    import cowsay

    return f"backcompat-venv|cowsay-ok|prefix={sys.prefix}"


with DAG(
    dag_id="s42_venv_backcompat",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_named",
        python_callable=use_named_env,
        venv="auto_19b97ee6bdb132eb",
    )
