from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def numpy_op():
    # numpy ships a compiled C-extension (.so). A real array reduction here proves
    # a wheelhouse-built compiled wheel actually LOADS + EXECUTES under the managed
    # venv at task runtime (the payoff of the glibc base switch, S37/S38).
    import sys
    import numpy as np

    total = int(np.arange(1_000_000).sum())  # deterministic: 499999500000
    return f"numpy={np.__version__}|sum={total}|prefix={sys.prefix}"


with DAG(
    dag_id="s42_numpy_rt2",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_numpy",
        python_callable=numpy_op,
        requirements=["numpy"],
    )
