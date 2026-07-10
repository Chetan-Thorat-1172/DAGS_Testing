from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="test_dagrun_timeout",
    schedule_interval=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    dagrun_timeout_seconds=60,
) as dag:

    t_long = BashOperator(
        task_id="t_long",
        bash_command="sleep 300",
    )

    t_after = BashOperator(
        task_id="t_after",
        bash_command="echo 'should never run'",
    )

    t_long >> t_after
