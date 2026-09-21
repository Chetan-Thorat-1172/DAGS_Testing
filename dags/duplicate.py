"""Calling Snowflake stored procedures with SQLExecuteQueryOperator.

Exercises the real-world shape: CALL of a procedure that takes time to run,
chained tasks, and object resolution both fully qualified and via a per-task
database/schema override.

Requires DEMO.PUBLIC.SP_WAIT_SHORT() and DEMO.PUBLIC.SP_WAIT_LONG().
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
)

CONN = "snowflake_conn"


def verify(**context):
    """Assert each CALL returned its procedure's string."""
    ti = context["ti"]

    def values(task_id):
        raw = ti.xcom_pull(task_ids=task_id, key="return_value")
        print(f"{task_id} -> {raw!r}")
        rows = json.loads(raw) if isinstance(raw, str) else raw
        # A CALL returns one row whose single column is named after the
        # procedure, so read the values rather than assume the column name.
        return [v for row in (rows or []) for v in row.values()]

    short = values("call_short")
    assert "SP_WAIT_SHORT completed" in short, f"call_short: {short}"

    long_ = values("call_long")
    assert "SP_WAIT_LONG completed" in long_, f"call_long: {long_}"

    # The unqualified CALL only resolves if the per-task database/schema
    # override actually applied to the session.
    unqual = values("call_unqualified")
    assert "SP_WAIT_SHORT completed" in unqual, f"call_unqualified: {unqual}"

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_call_procedure",
    description="Calls Snowflake stored procedures via SQLExecuteQueryOperator",
    tags=["livetest", "snowflake", "rbac_test"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # Fully qualified: needs no database on the connection.
    call_short = SQLExecuteQueryOperator(
        task_id="call_short",
        sql="CALL DEMO.PUBLIC.SP_WAIT_SHORT()",
    )

    call_long = SQLExecuteQueryOperator(
        task_id="call_long",
        sql="CALL DEMO.PUBLIC.SP_WAIT_LONG()",
    )

    # Unqualified: resolves only because the session is pointed at DEMO.PUBLIC
    # for this task alone.
    call_unqualified = SQLExecuteQueryOperator(
        task_id="call_unqualified",
        sql="CALL SP_WAIT_SHORT()",
        database="DEMO",
        schema="PUBLIC",
    )

    check = PythonOperator(
        task_id="verify",
        python_callable=verify,
        provide_context=True,
    )

    call_short >> call_long
    [call_long, call_unqualified] >> check
