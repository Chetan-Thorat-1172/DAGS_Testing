from dag_parser.dynamic.dag_context import DAG, BashOperator
from datetime import datetime

with DAG(
    dag_id="smoke_test_bash",
    schedule_interval=None,
    start_date=datetime(2026, 5, 19),
    catchup=False,
    description="Single-task smoke test for deployment verification",
) as dag:
    task = BashOperator(
        task_id="hello",
        bash_command="echo 'PI-FLOW smoke test OK' && date",
    )
