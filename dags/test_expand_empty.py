from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BashOperator

def process_item(op_kwargs=None, **kwargs):
    item = op_kwargs["item"] if op_kwargs else "unknown"
    return f"processed_{item}"


with DAG(
    dag_id="test_expand_empty",
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
        )
        .expand(
            op_kwargs=[]
        )
    )

    t_done = BashOperator(
        task_id="t_done",
        bash_command="echo done",
    )

    t_start >> t_mapped >> t_done
