from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="lt_0039",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 5, 14),
    catchup=False,
    max_active_runs=3,
    description="Load test: chain",
) as dag:
    t1 = BashOperator(task_id="step_1", bash_command="sleep 2 && echo done")
    t2 = BashOperator(task_id="step_2", bash_command="sleep 2 && echo done")
    t3 = BashOperator(task_id="step_3", bash_command="sleep 2 && echo done")
    t1 >> t2 >> t3
