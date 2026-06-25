from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonVirtualenvOperator


def use_managed_env():
    # piflow_spike exists ONLY inside the s40env venv that was BUILT IN-CONTAINER
    # and written to @VENV_STAGE via the writable (s3fs) mount this session. It is
    # absent from the base interpreter and from /opt/piflow/venv-extra, so a
    # successful import proves the worker resolved + ran under the managed venv
    # that landed on the stage through the RW mount (Fork 1B live proof, S40).
    import sys
    import piflow_spike

    return f"{piflow_spike.VALUE}|prefix={sys.prefix}"


with DAG(
    dag_id="s40_rwmount_venv",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:
    PythonVirtualenvOperator(
        task_id="t_managed",
        python_callable=use_managed_env,
        venv="s40env",
    )
