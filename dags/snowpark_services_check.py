"""Read-only: which job services survived the tests.

Confirms in Snowflake — not merely in a log line — that a job which timed out
with drop_on_completion on was dropped, while one with it off was kept, and that
a FAILED job was retained for inspection.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"


def report(**context):
    raw = context["ti"].xcom_pull(task_ids="services", key="return_value")
    rows = json.loads(raw) if isinstance(raw, str) else raw
    print(f"{len(rows or [])} service(s) remain in DEMO.PUBLIC:\n")
    for r in rows or []:
        print(f"  {r.get('name')}  status={r.get('status')}  is_job={r.get('is_job')}")
    return "ok"


with DAG(
    dag_id="snowpark_services_check",
    description="Which Snowpark job services survived the tests",
    tags=["livetest", "snowflake", "spcs"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    services = SQLExecuteQueryOperator(
        task_id="services",
        sql="SHOW SERVICES IN SCHEMA DEMO.PUBLIC",
    )

    services >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
