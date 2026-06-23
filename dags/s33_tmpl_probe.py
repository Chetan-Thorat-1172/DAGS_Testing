"""
S33 #6 silent-swallow depth probe.

bad_tmpl: op_kwargs val = "{{ .Pramas.x }}" (typo -> Go-template EXECUTE error).
  Pre-fix: silently passed the literal string -> task SUCCESS with wrong input.
  Post-fix: renderTemplates returns the error -> task FAILS LOUD (failed + DLQ),
  never dispatched. retries=0 for a deterministic single failure.

good_tmpl: op_kwargs val = "{{ .DS }}" (valid) -> must still render to the ds
  string and SUCCEED (proves valid templating is unaffected).
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator


def echo_val(**kwargs):
    return {"got": kwargs.get("val")}


with DAG(
    dag_id="s33_tmpl_probe",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="S33 #6 template silent-swallow -> fail-loud probe",
    tags=["regression", "cat3", "python", "s33", "templating"],
) as dag:
    PythonOperator(
        task_id="bad_tmpl",
        python_callable=echo_val,
        op_kwargs={"val": "{{ .Pramas.x }}"},
        retries=0,
    )
    PythonOperator(
        task_id="good_tmpl",
        python_callable=echo_val,
        op_kwargs={"val": "{{ .DS }}"},
    )
