from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
    dag_id="ver_midflight_test",
    schedule_interval="@once",
    start_date=datetime(2026, 6, 22),
    catchup=False,
) as dag:
    t1 = BashOperator(task_id="start", bash_command="echo 'V2 started'")
    t2 = BashOperator(task_id="slow", bash_command="sleep 360 && echo 'V2 slow done'")
    t4 = BashOperator(task_id="new_task", bash_command="echo 'V2 new task'")
    
    t1 >> t2 >> t4
