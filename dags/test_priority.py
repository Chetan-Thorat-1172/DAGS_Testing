from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="test_priority",
    schedule_interval=None,
    start_date=datetime(2026, 7, 2),
    catchup=False,
) as dag:
    t_low = BashOperator(
        task_id="t_low",
        bash_command="echo low",
        priority_weight=1,
    )

    t_med = BashOperator(
        task_id="t_med",
        bash_command="echo medium",
        priority_weight=5,
    )

    t_high = BashOperator(
        task_id="t_high",
        bash_command="echo high",
        priority_weight=10,
    )
