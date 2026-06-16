"""xcom_01_auto_return_value.py — Test #1: Automatic return_value push

WHAT WE'RE TESTING:
  When a PythonOperator callable RETURNS a value, PI_FLOW automatically pushes it
  to the XCom table under the key "return_value".

HOW IT WORKS INTERNALLY:
  1. Worker runs the PythonOperator via _run_task.py (subprocess)
  2. The callable returns a dict → _run_task.py serializes it to JSON
  3. _run_task.py outputs: {"status":"success", "return_value": "<JSON>", "error":""}
  4. Go's PythonExecutor reads stdout, gets the return_value string
  5. task_runner.go line ~234: pushes it to xcom table via XComRepository.PushXCom()
     INSERT INTO metadata.xcom (dag_id, task_id, run_id, map_index, key, value)
     VALUES (..., ..., ..., -1, 'return_value', '<JSON>')
     ON CONFLICT ... DO UPDATE

HOW TO VERIFY:
  After the run succeeds, query the DB:
    SELECT * FROM metadata.xcom
    WHERE dag_id = 'xcom_01_auto_return' AND key = 'return_value';
  Expected: value = {"rows": 100, "source": "api", "status": "ok"}

  Also check the PI_FLOW UI → Task Instance → XCom tab (if available).
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="xcom_01_auto_return",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    def produce_data(**context):
        """This return value is automatically pushed to XCom as 'return_value'."""
        result = {"rows": 100, "source": "api", "status": "ok"}
        print(f"Producing: {result}")
        return result  # ← this gets auto-pushed to xcom

    producer = PythonOperator(task_id="producer", python_callable=produce_data)
