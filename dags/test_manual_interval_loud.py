from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator

# S37 warm-up probe (#5 b-ii manual-run-stays-loud, live). schedule_interval=None
# => manual-only, so the ONLY runs are the ones seeded by hand with NULL
# data_interval_*. op_kwargs is a template_field, so {{ data_interval_start }} is
# rendered with PI-FLOW's loud Undefined; absent on a manual run -> UndefinedError
# -> task fails loud (NOT a silent empty render).


def echo_interval(dis=None, **kwargs):
    return f"data_interval_start={dis}"


with DAG(
    dag_id="test_manual_interval_loud",
    schedule_interval=None,
    start_date=datetime(2026, 6, 25),
    catchup=False,
) as dag:

    t = PythonOperator(
        task_id="template_dis",
        python_callable=echo_interval,
        op_kwargs={"dis": "{{ data_interval_start }}"},
        retries=0,
    )
