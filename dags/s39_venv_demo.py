from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def use_managed_env(**kwargs):
    # piflow_demo lives ONLY in the named managed venv (@VENV_STAGE/demoenv),
    # absent from the base image AND /opt/piflow/venv-extra. Imported INSIDE the
    # callable so the base-interpreter parse at ingestion still succeeds.
    import sys

    import piflow_demo

    return (
        piflow_demo.describe(123456789)
        + "|prefix=" + sys.prefix
        + "|ver=" + piflow_demo.__version__
    )


with DAG(
    dag_id="s39_venv_demo",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    t_managed = PythonVirtualenvOperator(
        task_id="t_managed",
        python_callable=use_managed_env,
        venv="demoenv",
    )
