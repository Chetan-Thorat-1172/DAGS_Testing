from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def use_managed_env():
    # S41 Fork 2, Option 2: the author writes Airflow-identical code —
    # PythonVirtualenvOperator(requirements=[...]). PI-FLOW derives the
    # auto_<hash> env name, an admin built it once from the WHEELHOUSE
    # (no PyPI egress), and the worker resolves + runs the callable under it.
    # cowsay is absent from the base interpreter and from /opt/piflow/venv-extra,
    # so a successful import + a sys.prefix under the managed-venv cache proves
    # the requirements=[...] path resolved the wheelhouse-built env end-to-end.
    import sys
    import importlib.metadata as md

    import cowsay

    return f"s41-option2|cowsay={md.version('cowsay')}|prefix={sys.prefix}"


with DAG(
    dag_id="s41_fork2_requirements",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_req",
        python_callable=use_managed_env,
        requirements=["cowsay==6.1"],
    )
