"""xcom_04_custom_key_pull.py — Test #4: Pull a custom key from upstream

WHAT WE'RE TESTING:
  Consumer task pulls a CUSTOM key (not just "return_value") that was explicitly
  pushed by the producer using ti.xcom_push(key="custom_key", value=...).

HOW IT WORKS INTERNALLY:
  Same as Test #3 but with key != "return_value":
  - Producer: ti.xcom_push(key="batch_id", value="...")
  - Consumer: ti.xcom_pull(task_ids="producer", key="batch_id")
  - Internal HTTP: GET /internal/xcom/pull?...&key=batch_id

HOW TO VERIFY:
  - Both tasks succeed
  - Consumer logs: "Got batch_id: 2026-06-15-batch-A" and "Got row_count: 42"
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="xcom_04_custom_key_pull",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    def produce(**context):
        ti = context["ti"]
        ti.xcom_push(key="batch_id", value="2026-06-15-batch-A")
        ti.xcom_push(key="row_count", value=42)
        print("Pushed batch_id and row_count")
        return "producer done"

    def consume(**context):
        import json
        ti = context["ti"]
        batch = ti.xcom_pull(task_ids="producer", key="batch_id")
        count = ti.xcom_pull(task_ids="producer", key="row_count")

        # Defensively parse — values may arrive as JSON strings
        if isinstance(batch, str):
            try: batch = json.loads(batch)
            except: pass
        if isinstance(count, str):
            try: count = json.loads(count)
            except: pass

        print(f"Got batch_id: {batch}")
        print(f"Got row_count: {count}")

        if batch is None:
            raise ValueError("batch_id xcom_pull returned None!")
        if count is None:
            raise ValueError("row_count xcom_pull returned None!")

        return {"batch": batch, "count": count}

    producer = PythonOperator(task_id="producer", python_callable=produce)
    consumer = PythonOperator(task_id="consumer", python_callable=consume)

    producer >> consumer
