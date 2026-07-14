from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    BashOperator,
)

with DAG(
    dag_id="test_catchup_toggle",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 10),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id="process",
        bash_command="echo hello",
    )
