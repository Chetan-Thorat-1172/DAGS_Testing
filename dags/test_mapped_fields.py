from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator


def process_item(op_kwargs=None, **kwargs):
    item = op_kwargs["item"] if op_kwargs else "unknown"
    return f"processed_{item}"


with DAG(
    dag_id="test_mapped_fields",
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
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
            retries=3,
            retry_delay_seconds=60,
            retry_exponential_backoff=True,
            max_retry_delay_seconds=300,
            execution_timeout=120,
            trigger_rule="all_success",
            pool="default",
            priority_weight=5,
        )
        .expand(
            op_kwargs=[
                {"item": "apple"},
                {"item": "banana"},
            ]
        )
    )

    t_start >> t_mapped
