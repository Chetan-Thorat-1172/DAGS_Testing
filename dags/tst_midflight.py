from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
      dag_id="tst_midflight",
      schedule_interval="@once",
      start_date=datetime(2026, 6, 23),
      catchup=False,
) as dag:
    t1 = BashOperator(task_id="start", bash_command="echo 'V1 started'")
    t2 = BashOperator(task_id="slow", bash_command="sleep 360 && echo 'V1 slow done'")
    t3 = BashOperator(task_id="finish", bash_command="echo 'V1 finish'")
    
    t1 >> t2 >> t3
