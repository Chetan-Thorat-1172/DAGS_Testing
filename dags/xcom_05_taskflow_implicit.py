"""xcom_05_taskflow_implicit.py — Test #5: TaskFlow API implicit XCom wiring

WHAT WE'RE TESTING:
  Using @task decorator, passing one task's result directly as another task's argument
  automatically wires XCom — no explicit ti.xcom_push/pull needed.

HOW IT WORKS INTERNALLY:
  1. @task decorator wraps each function as a PythonOperator
  2. When you write: result = extract() → transform(result)
     - extract()'s return value is auto-pushed as XCom return_value
     - transform's argument is recorded as an XComArg reference:
       {"__xcom_ref__": true, "task_id": "extract", "key": "return_value"}
  3. At runtime, _run_task.py (line ~267) resolves XCom references BEFORE calling:
     - Sees __xcom_ref__ in op_kwargs → calls ti.xcom_pull(task_ids="extract")
     - Replaces the reference with the actual value
     - Calls transform(data=<actual_value>)
  4. Dependencies are inferred: extract >> transform (no explicit >> needed)

HOW TO VERIFY:
  - All 3 tasks succeed (extract, transform, load)
  - Check task logs for each step:
    extract: "Extracted 100 rows"
    transform: "Transforming: {'rows': 100, ...}"
    load: "Loading 95 rows"
  - DB: xcom table has return_value for each task
"""
from datetime import datetime
from dag_parser.dynamic.dag_context import dag, task


@dag(
    dag_id="xcom_05_taskflow_implicit",
    schedule=None,
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["xcom", "test"],
)
def pipeline():

    @task
    def extract():
        print("Extracted 100 rows")
        return {"rows": 100, "source": "api"}

    @task
    def transform(data):
        print(f"Transforming: {data}")
        cleaned = data["rows"] - 5
        return {"cleaned_rows": cleaned}

    @task
    def load(data):
        print(f"Loading {data['cleaned_rows']} rows")
        return {"loaded": data["cleaned_rows"]}

    raw = extract()          # returns XComArg → dependency inferred
    cleaned = transform(raw) # receives extract's return_value via XCom
    load(cleaned)            # receives transform's return_value via XCom


pipeline()  # MUST call the @dag function to create the DAG
