"""Feature 35 - XCom.

Tasks run in separate processes, so they cannot share variables. XCom is the
little postbox they use to pass small values to each other.

    extract  ->  transform  ->  report

extract   pushes one value explicitly, and RETURNS another (auto-pushed)
transform pulls both, and shows the two traps
report    reads transform's return value
"""

from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def extract(**context):
    ti = context["ti"]
    # 1. Explicit push, under a key I choose.
    ti.xcom_push(key="row_count", value=42)
    # 2. Whatever I RETURN is auto-pushed under the key "return_value".
    return {"table": "orders", "status": "ok"}


def transform(**context):
    ti = context["ti"]

    # THE TRAP: pulling from a normal (unmapped) task without map_indexes=-1
    wrong = ti.xcom_pull(task_ids="extract", key="row_count")
    print(f"without map_indexes=-1 -> {wrong!r}", flush=True)

    # THE FIX
    count = ti.xcom_pull(task_ids="extract", key="row_count", map_indexes=-1)
    payload = ti.xcom_pull(task_ids="extract", key="return_value", map_indexes=-1)
    print(f"with    map_indexes=-1 -> {count!r} and {payload!r}", flush=True)

    return {"rows_seen": count, "from_table": payload.get("table")}


def report(**context):
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="transform", key="return_value", map_indexes=-1)
    print(f"report got: {result!r}", flush=True)


with DAG(
    dag_id="07_xcom",
    description="Feature 35 - passing values between tasks",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
) as dag:

    e = PythonOperator(task_id="extract", python_callable=extract, provide_context=True)
    t = PythonOperator(task_id="transform", python_callable=transform, provide_context=True)
    r = PythonOperator(task_id="report", python_callable=report, provide_context=True)

    e >> t >> r
