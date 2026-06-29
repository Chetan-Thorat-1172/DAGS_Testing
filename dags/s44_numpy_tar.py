"""
s44_numpy_tar.py - LIVE proof for S44 tar-stream materialization.

PythonVirtualenvOperator with requirements=["numpy"]. After the S44 builder change,
this env is shipped as a single <name>.tar.gz on the stage; the worker streams +
extracts it once on first use instead of the slow per-file FUSE copy.

Baseline (S42, file-by-file copy): numpy first-use ~655s.
Expected (S44, tar-stream): first-use materialization in seconds.

Measure: first run's task_instance duration (materialization happens in the worker
before the callable runs). XCOM sum must equal 499999500000 (deterministic).
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def numpy_op():
    import sys
    import numpy as np

    total = int(np.arange(1_000_000).sum())  # deterministic: 499999500000
    return f"numpy={np.__version__}|sum={total}|prefix={sys.prefix}"


with DAG(
    dag_id="s44_numpy_tar",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["s44", "materialization", "tar-stream"],
) as dag:
    PythonVirtualenvOperator(
        task_id="t_numpy",
        python_callable=numpy_op,
        requirements=["numpy==2.2.6"],
    )
