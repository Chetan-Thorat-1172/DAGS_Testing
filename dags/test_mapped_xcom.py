from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator


def process_item(op_kwargs=None, **kwargs):
    item = op_kwargs["item"] if op_kwargs else "unknown"
    return f"processed_{item}"


def aggregate(**kwargs):
    ti = kwargs["ti"]

    # Pull all mapped task results at once
    all_results = ti.xcom_pull(
        task_ids="t_process",
        key="return_value",
    )

    print(f"Aggregated results: {all_results}", flush=True)

    return f"total={all_results}"


with DAG(
    dag_id="test_mapped_xcom",
    schedule_interval=None,
    start_date=datetime(2026, 7, 2),
    catchup=False,
) as dag:

    t_start = BashOperator(
        task_id="t_start",
        bash_command="echo start",
    )

    t_mapped = (
        PythonOperator(
            task_id="t_process",
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
        python_callable=aggregate,
    )

    t_start >> t_mapped >> t_aggregate
