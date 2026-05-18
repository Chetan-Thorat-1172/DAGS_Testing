from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="lt_0024",
    schedule_interval="54 17 * * *",
    start_date=datetime(2026, 5, 15),
    catchup=False,
    max_active_runs=3,
    description="Load test: 5-task python chain",
) as dag:
    t1 = PythonOperator(task_id="step_1", python_callable="import time; time.sleep(1); print('step_1_done')")
    t2 = PythonOperator(task_id="step_2", python_callable="import time; time.sleep(1); print('step_2_done')")
    t3 = PythonOperator(task_id="step_3", python_callable="import time; time.sleep(1); print('step_3_done')")
    t4 = PythonOperator(task_id="step_4", python_callable="import time; time.sleep(1); print('step_4_done')")
    t5 = PythonOperator(task_id="step_5", python_callable="import time; time.sleep(1); print('step_5_done')")
    t1 >> t2 >> t3 >> t4 >> t5
