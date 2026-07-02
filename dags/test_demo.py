from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_demo",
    schedule_interval=None,
    start_date=datetime(2026, 7, 2),
    catchup=False,
) as dag:

    t_1 = BashOperator(
        task_id="t_long",
        bash_command="slep 90",
    )
    t_2 = BashOperator(
            task_id="t_long",
            bash_command="sleep 10",
            retries=1,
            retry_delay_seconds=5,
    )

    t_1 >> t_2
