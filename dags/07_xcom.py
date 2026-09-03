"""Feature 35 - XCom.

Tasks run in separate processes, so they cannot share a variable. XCom is the
small postbox they use to pass values to each other.

    extract  ->  load

extract counts some rows and returns the number.
load picks that number up and uses it.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import DAG, PythonOperator


def extract():
    row_count = 42
    print(f"extracted {row_count} rows", flush=True)
    return row_count          # whatever you return is put in the postbox


def load(**context):
    ti = context["ti"]
    row_count = json.loads(
        ti.xcom_pull(task_ids="extract", key="return_value", map_indexes=-1)
    )
    print(f"loading {row_count} rows", flush=True)


with DAG(
    dag_id="07_xcom",
    description="Feature 35 - passing a value between tasks",
    schedule=None,
    start_date=datetime(2026, 9, 2),
    catchup=False,
    tags=["session"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    load_task = PythonOperator(task_id="load", python_callable=load, provide_context=True)

    extract_task >> load_task
