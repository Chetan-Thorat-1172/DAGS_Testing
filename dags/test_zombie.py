from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_zombie",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 2),
    catchup=False,
) as dag:

    t_long = BashOperator(
        task_id="t_long",
        bash_command="sleep 90",
        retries=1,
        retry_delay_seconds=5,
    )
