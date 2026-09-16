"""Read-only probe: what does the connection's session actually see?

Used to decide where the data-quality test tables should live. Creates nothing.
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
    ti = context["ti"]
    for task_id in ("session_context", "visible_databases"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        rows = json.loads(raw) if isinstance(raw, str) else raw
        print(f"--- {task_id} ---")
        for r in rows or []:
            print(r)
    return "ok"


with DAG(
    dag_id="snowflake_probe_context",
    description="Read-only probe of the Snowflake session context",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    session_context = SQLExecuteQueryOperator(
        task_id="session_context",
        sql=(
            "SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_USER() AS USR, "
            "CURRENT_ROLE() AS ROLE, CURRENT_WAREHOUSE() AS WH, "
            "CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCH"
        ),
    )

    visible_databases = SQLExecuteQueryOperator(
        task_id="visible_databases",
        sql=(
            "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES "
            "ORDER BY DATABASE_NAME"
        ),
    )

    [session_context, visible_databases] >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
