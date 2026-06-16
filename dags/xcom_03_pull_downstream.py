"""xcom_03_pull_downstream.py — Test #3: xcom_pull in a downstream task

WHAT WE'RE TESTING:
  A downstream task can PULL an upstream task's XCom value using:
    ti.xcom_pull(task_ids="upstream_task_id", key="return_value")

HOW IT WORKS INTERNALLY:
  1. 'producer' runs first, returns a dict → auto-pushed as return_value
  2. 'consumer' runs next (dependency: producer >> consumer)
  3. Inside consumer, ti.xcom_pull() makes an HTTP GET to:
     http://127.0.0.1:8083/internal/xcom/pull?dag_id=...&run_id=...&task_id=producer&key=return_value
  4. The Go server's XComInternalHandler.Pull() queries the xcom table
  5. Returns the value → _run_task.py deserializes JSON → callable receives it

HOW TO VERIFY:
  - Both tasks should succeed
  - Check consumer task LOGS — it should print the pulled value:
    "Pulled from producer: {'rows': 100, 'source': 'api'}"
  - The consumer's own return_value in xcom should contain the confirmation
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator

with DAG(
    dag_id="xcom_03_pull_downstream",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
) as dag:

    def produce(**context):
        return {"rows": 100, "source": "api"}

    def consume(**context):
        ti = context["ti"]

        # Pull the upstream task's return_value
        data = ti.xcom_pull(task_ids="producer", key="return_value")
        print(f"Pulled from producer: {data}")

        if data is None:
            raise ValueError("xcom_pull returned None — XCom not found!")

        # Verify we got what we expected
        assert data["rows"] == 100, f"Expected rows=100, got {data['rows']}"
        assert data["source"] == "api", f"Expected source='api', got {data['source']}"

        return {"status": "verified", "received_rows": data["rows"]}

    producer = PythonOperator(task_id="producer", python_callable=produce)
    consumer = PythonOperator(task_id="consumer", python_callable=consume)

    producer >> consumer
