from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    ExternalPythonOperator,
    PythonOperator,
)

# S37 alpine->glibc gate proof. /opt/piflow/venv-extra is the baked second
# interpreter (ExternalPythonOperator target). numpy is a manylinux/glibc-only
# compiled wheel that does NOT install on the old alpine/musl base; cowsay is the
# S34 continuity package. Imports are INSIDE the callables because the worker
# re-imports this DAG module in BOTH the base interpreter (parse/ingest) and the
# target interpreter, and the base python3 has neither package.
VENV = "/opt/piflow/venv-extra/bin/python3"


def numpy_via_venv():
    import numpy as np

    arr = np.arange(5)
    return f"numpy={np.__version__} sum={int(arr.sum())}"


def cowsay_via_venv():
    import cowsay

    return f"cowsay_imported_from={cowsay.__file__}"


def numpy_on_base():
    # Runs under the BASE python3 (plain PythonOperator). The base image does NOT
    # bake numpy, so this import MUST fail -- the live contrast that makes the
    # ExternalPythonOperator numpy success meaningful (it ran under venv-extra).
    import numpy as np

    return f"unexpected_base_numpy={np.__version__}"


with DAG(
    dag_id="test_glibc_venv_extra",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    t_numpy_venv = ExternalPythonOperator(
        task_id="numpy_via_venv",
        python_callable=numpy_via_venv,
        python=VENV,
        retries=0,
    )

    t_cowsay_venv = ExternalPythonOperator(
        task_id="cowsay_via_venv",
        python_callable=cowsay_via_venv,
        python=VENV,
        retries=0,
    )

    t_numpy_base = PythonOperator(
        task_id="numpy_on_base_should_fail",
        python_callable=numpy_on_base,
        retries=0,
    )
