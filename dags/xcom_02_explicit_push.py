"""xcom_02_explicit_push.py — Test #2: Explicit xcom_push with custom key

WHAT WE'RE TESTING:
  A task can push MULTIPLE values to XCom using ti.xcom_push(key, value) — not just
  the automatic return_value. Each push creates a separate row in the xcom table.

HOW IT WORKS INTERNALLY:
  1. The callable receives context["ti"] — a TaskInstanceMock object (_run_task.py:208)
  2. ti.xcom_push(key, value) makes an HTTP POST to http://127.0.0.1:8083/internal/xcom/push
     with payload: {"dag_id":"...", "run_id":"...", "task_id":"...", "key":"batch_id", "value":"..."}
  3. The Go server's XComInternalHandler.Push() calls XComRepository.PushXCom()
  4. This does an UPSERT into metadata.xcom with the custom key

HOW TO VERIFY:
  After the run succeeds, query:
    SELECT * FROM metadata.xcom
    WHERE dag_id = 'xcom_02_explicit_push' AND task_id = 'producer';
  Expected: TWO rows:
    - key='return_value', value={"final": "done"}       (auto from return)
    - key='batch_id', value="2026-06-15-batch-A"        (explicit push)
    - key='row_count', value=42                         (explicit push)
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="xcom_02_explicit_push",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    def produce_multiple(**context):
        ti = context["ti"]

        # Explicit push with custom keys
        ti.xcom_push(key="batch_id", value="2026-06-15-batch-A")
        ti.xcom_push(key="row_count", value=42)

        print("Pushed batch_id and row_count via ti.xcom_push()")

        # This return also gets auto-pushed as 'return_value'
        return {"final": "done"}

    producer = PythonOperator(task_id="producer", python_callable=produce_multiple)
