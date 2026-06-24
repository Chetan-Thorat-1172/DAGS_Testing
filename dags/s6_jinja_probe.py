"""WI-JINJA live probe: PythonOperator template_fields rendered by real Jinja2.
Proves ds / macros.ds_add / filter / piflow.* alias / var.value / dag_run.conf
render through the new engine and persist to XCOM. Manual-run only."""
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def render_probe(**kwargs):
    # Return the rendered op_kwargs so they land in XCOM (return_value) as JSONB.
    return {
        "ds": kwargs.get("k_ds"),
        "ds_add7": kwargs.get("k_add7"),
        "upper_conf": kwargs.get("k_up"),
        "piflow_ds": kwargs.get("k_pf"),
        "filtered": kwargs.get("k_flt"),
        "nested_list": kwargs.get("k_list"),
    }


with DAG(
    dag_id="s6_jinja_probe",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    user_defined_filters={"shout": lambda s: str(s).upper() + "!"},
) as dag:
    PythonOperator(
        task_id="probe",
        python_callable=render_probe,
        op_kwargs={
            "k_ds": "{{ ds }}",
            "k_add7": "{{ macros.ds_add(ds, 7) }}",
            "k_up": "{{ dag_run.conf.get('name', 'none') | upper }}",
            "k_pf": "{{ piflow.ds }}",
            "k_flt": "{{ 'abc' | shout }}",
            "k_list": ["{{ ds }}", "plain", "{{ macros.ds_add(ds, 1) }}"],
        },
    )
