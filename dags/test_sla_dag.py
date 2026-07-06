from datetime import datetime
  import time
  from dag_parser.dynamic.dag_context import DAG, PythonOperator

  def slow_task(**ctx):
      time.sleep(30)   # deliberately slow
      return "done"

  with DAG(
      dag_id="test_sla_dag",
      schedule=None,
      start_date=datetime(2026, 7, 6),
      catchup=False,
      expected_duration_seconds=5,   # SLA = 5s; task sleeps 30s → guaranteed miss
  ) as dag:
      PythonOperator(task_id="slow", python_callable=slow_task)
