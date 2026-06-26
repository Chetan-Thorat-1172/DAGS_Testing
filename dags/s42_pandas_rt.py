from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def pandas_op():
    # pandas pulls a real dep tree (numpy, python-dateutil, pytz, tzdata, six) and
    # is compiled — proves multi-wheel dependency resolution from the wheelhouse
    # plus a compiled extension executing at runtime.
    import sys
    import pandas as pd

    df = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]})
    g = df.groupby("k")["v"].sum().to_dict()  # {"a":3,"b":3}
    return f"pandas={pd.__version__}|groupby={g}|prefix={sys.prefix}"


with DAG(
    dag_id="s42_pandas_rt",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 26),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_pandas",
        python_callable=pandas_op,
        requirements=["pandas"],
    )
