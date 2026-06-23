from dag_parser.dynamic.dag_context import DAG, ExternalPythonOperator, PythonOperator
from datetime import datetime


def ext_probe():
    # Runs under /opt/piflow/venv-extra/bin/python3 (ExternalPythonOperator).
    # cowsay is installed ONLY in that venv, not the base interpreter -> proves
    # per-task interpreter isolation. sys.executable proves which interpreter ran.
    import sys
    try:
        import cowsay  # noqa: F401
        has = True
        out = cowsay.get_output_string("cow", "ext") if hasattr(cowsay, "get_output_string") else "ok"
    except Exception:
        has = False
        out = None
    return {"interp": sys.executable, "has_cowsay": has, "sample_len": len(out) if out else 0}


def base_probe():
    # Runs under the baked base python3 (plain PythonOperator). cowsay must be ABSENT.
    import sys
    try:
        import cowsay  # noqa: F401
        has = True
    except Exception:
        has = False
    return {"interp": sys.executable, "has_cowsay": has}


with DAG(
    dag_id="s34_extpy",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    ExternalPythonOperator(
        task_id="ext",
        python_callable=ext_probe,
        python="/opt/piflow/venv-extra/bin/python3",
    )
    PythonOperator(task_id="base", python_callable=base_probe)
