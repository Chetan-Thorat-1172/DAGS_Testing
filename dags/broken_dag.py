from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, BashOperator


with DAG(
    dag_id="broken_dag"
    # Missing comma after dag_id — SyntaxError
    schedule_interval="@once",
    start_date=datetime(2026, 6, 25),
) as dag:
    BashOperator(
        task_id="t1",
        bash_command="echo hi",
    )
