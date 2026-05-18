from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="lt_0001",
    schedule_interval="55 12 * * *",
    start_date=datetime(2026, 5, 18),
    catchup=False,
    max_active_runs=3,
    description="Load test: 5-task bash chain",
) as dag:
    t1 = BashOperator(task_id="step_1", bash_command="sleep 1 && echo step_1_done")
    t2 = BashOperator(task_id="step_2", bash_command="sleep 1 && echo step_2_done")
    t3 = BashOperator(task_id="step_3", bash_command="sleep 1 && echo step_3_done")
    t4 = BashOperator(task_id="step_4", bash_command="sleep 1 && echo step_4_done")
    t5 = BashOperator(task_id="step_5", bash_command="sleep 1 && echo step_5_done")
    t1 >> t2 >> t3 >> t4 >> t5
