from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="test_dop_fail",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 21),
    catchup=True,
) as dag:

    t1 = BashOperator(
        task_id="always_ok",
        bash_command="echo ok",
    )

    t2 = BashOperator(
        task_id="gated_task",
        bash_command="exit 1",
        depends_on_past=True,
        retries=0,
    )

    t1 >> t2
