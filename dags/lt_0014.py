from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="lt_0014",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 5, 14),
    catchup=False,
    description="Load test: single task",
) as dag:
    t1 = BashOperator(task_id="run", bash_command="sleep 1 && echo done")
