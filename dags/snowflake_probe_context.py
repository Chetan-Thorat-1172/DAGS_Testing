"""Read-only probe of the Snowflake session and the sample dataset.

Used to pin the real row counts the data-quality checks assert against, so the
expected values come from the data rather than from memory. Creates nothing.
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
    for task_id in ("session_context", "visible_databases", "tpch_counts", "tpch_quality"):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        rows = json.loads(raw) if isinstance(raw, str) else raw
        print(f"--- {task_id} ---")
        for r in rows or []:
            print(r)
    return "ok"


with DAG(
    dag_id="snowflake_probe_context",
    description="Read-only probe of the Snowflake session context and sample data",
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

    # The exact counts the value checks will assert against.
    tpch_counts = SQLExecuteQueryOperator(
        task_id="tpch_counts",
        sql=(
            "SELECT "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION) AS REGION_N, "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION) AS NATION_N, "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER) AS CUSTOMER_N, "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS) AS ORDERS_N, "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER) AS SUPPLIER_N"
        ),
    )

    # Whether the real data is actually clean on the dimensions the checks test.
    tpch_quality = SQLExecuteQueryOperator(
        task_id="tpch_quality",
        sql=(
            "SELECT "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER "
            " WHERE C_CUSTKEY IS NULL) AS NULL_KEYS, "
            "(SELECT COUNT(*) - COUNT(DISTINCT C_CUSTKEY) "
            " FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER) AS DUP_KEYS, "
            "(SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o "
            " LEFT JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c "
            "   ON o.O_CUSTKEY = c.C_CUSTKEY WHERE c.C_CUSTKEY IS NULL) AS ORPHAN_ORDERS, "
            "(SELECT ROUND(SUM(O_TOTALPRICE)) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS) AS TOTAL_REVENUE"
        ),
    )

    [session_context, visible_databases, tpch_counts, tpch_quality] >> PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True,
    )
