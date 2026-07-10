from datetime import datetime
from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    BashOperator,
)

def process_item(item, **kwargs):
    return f"processed_{item}"

def aggregate_all(**kwargs):
    ti = kwargs["ti"]

    all_results = ti.xcom_pull(
        task_ids="t_mapped",
        key="return_value",
    )

    print(f"All mapped results: {all_results}", flush=True)
    return str(all_results)

with DAG(
    dag_id="test_mapped_xcom3",
    schedule_interval="@once",
    start_date=datetime(2026, 7, 10),
    catchup=False,
) as dag:

    # Dummy upstream forces planner to leave t_mapped in 'none'
    # so the expander (Phase 2.7) can expand it before dispatch
    t_start = BashOperator(
        task_id="t_start",
        bash_command="echo start",
    )

    t_mapped = (
        PythonOperator(
            task_id="t_mapped",
            python_callable=process_item,
        )
        .expand(
            op_kwargs=[
                {"item": "apple"},
                {"item": "banana"},
                {"item": "cherry"},
            ]
        )
    )

    t_aggregate = PythonOperator(
        task_id="t_aggregate",
        python_callable=aggregate_all,
    )

    t_start >> t_mapped >> t_aggregate
