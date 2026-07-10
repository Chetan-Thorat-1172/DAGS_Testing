from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator

def process_item(op_kwargs=None, **kwargs):
    item = op_kwargs.get("item", "unknown")
    mode = op_kwargs.get("mode", "unknown")
    retries = op_kwargs.get("max_retries", 0)

    print(
        f"Processing: {item}, mode={mode}, max_retries={retries}",
        flush=True,
    )

    return f"{item}_{mode}_{retries}"


with DAG(
    dag_id="test_partial",
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
        .partial(
            op_kwargs={
                "mode": "fast",
                "max_retries": 3,
            }
        )
        .expand(
            op_kwargs=[
                {"item": "apple"},
                {"item": "banana"},
                {"item": "cherry"},
            ]
        )
    )

    t_start >> t_mapped
