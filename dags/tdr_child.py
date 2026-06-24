from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="tdr_child",
    schedule_interval=None,
    start_date=datetime(2026, 6, 24),
    catchup=False,
) as dag:
    BashOperator(
        task_id="child_work",
        bash_command="echo 'child DAG executed'"
    )
