from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, BashOperator

with DAG(
        dag_id="test_clear_task",
        schedule=None,
        start_date=datetime(2026, 7, 1),
        catchup=False,
) as dag:
      t1 = BashOperator(task_id="t1", bash_command="echo t1")
      t2 = BashOperator(task_id="t2", bash_command="echo t2")
      t3 = BashOperator(task_id="t3", bash_command="echo t3")
      t1 >> t2 >> t3
