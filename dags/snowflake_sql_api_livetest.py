"""Live end-to-end test for SnowflakeSqlApiOperator.

This is the first use of the SQL REST API client in the shared hook — Submit,
Status, Result and Cancel shipped with it but have never run against Snowflake
until now. So this exercises the operator and that client together.

Works against DEMO.PUBLIC.SQLAPI_TARGET, created by the first task, so it needs
no setup beyond the connection.

A green run is the assertion. Deliberate failures live in
snowflake_sql_api_rejects.py.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SnowflakeSqlApiOperator,
    SnowflakeValueCheckOperator,
)

CONN = "snowflake_conn"
SESSION = {"database": "DEMO", "schema": "PUBLIC"}
TARGET = "DEMO.PUBLIC.SQLAPI_TARGET"


def report(**context):
    """The handles are pushed under `query_ids`, not as the return value."""
    ti = context["ti"]

    for task_id in ("create_table", "multi_statement", "with_bindings"):
        raw = ti.xcom_pull(task_ids=task_id, key="query_ids")
        print(f"{task_id} -> query_ids = {raw!r}")
        assert raw, f"{task_id} pushed no query_ids"
        handles = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(handles, list) and handles, f"{task_id}: {handles}"
        print(f"  {len(handles)} handle(s)")

    # A statement handle is what Snowflake's query history is keyed on, so it
    # should look like a UUID rather than an opaque token.
    handles = json.loads(ti.xcom_pull(task_ids="multi_statement", key="query_ids"))
    print(f"multi_statement handles: {handles}")

    # The task returns no value of its own — only the named XCom.
    assert ti.xcom_pull(task_ids="create_table", key="return_value") is None, \
        "the operator should push no return_value"

    print("ALL ASSERTIONS PASSED")
    return "ok"


with DAG(
    dag_id="snowflake_sql_api_livetest",
    description="Running SQL through the Snowflake SQL REST API",
    tags=["livetest", "snowflake", "sqlapi"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # A single statement. Also creates what the later tasks use.
    create_table = SnowflakeSqlApiOperator(
        task_id="create_table",
        sql=f"CREATE OR REPLACE TABLE {TARGET} (ID NUMBER, NAME VARCHAR)",
        statement_count=1,
        **SESSION,
    )

    # Several statements in ONE request — the reason this operator exists.
    # statement_count must match, or Snowflake rejects the submission.
    multi_statement = SnowflakeSqlApiOperator(
        task_id="multi_statement",
        sql=(
            f"INSERT INTO {TARGET} VALUES (1, 'one'); "
            f"INSERT INTO {TARGET} VALUES (2, 'two'); "
            f"INSERT INTO {TARGET} VALUES (3, 'three');"
        ),
        statement_count=3,
        **SESSION,
    )

    # 0 lets Snowflake accept however many statements arrive.
    variable_count = SnowflakeSqlApiOperator(
        task_id="variable_count",
        sql=(
            f"INSERT INTO {TARGET} VALUES (4, 'four'); "
            f"INSERT INTO {TARGET} VALUES (5, 'five');"
        ),
        statement_count=0,
        **SESSION,
    )

    # Bind variables, which the API takes as {type, value} pairs keyed by
    # position. Single-statement, since Snowflake ignores bindings otherwise.
    with_bindings = SnowflakeSqlApiOperator(
        task_id="with_bindings",
        sql=f"INSERT INTO {TARGET} VALUES (?, ?)",
        statement_count=1,
        bindings={
            "1": {"type": "FIXED", "value": "6"},
            "2": {"type": "TEXT", "value": "six"},
        },
        **SESSION,
    )

    # A server-side statement timeout, generous enough not to trip.
    with_timeout = SnowflakeSqlApiOperator(
        task_id="with_timeout",
        sql=f"SELECT COUNT(*) FROM {TARGET}",
        statement_count=1,
        timeout=120,
        poll_interval=2,
        **SESSION,
    )

    # Everything above landed: 3 + 2 + 1 = 6 rows. Checked with the operator
    # built for it, which also proves the two paths agree.
    check_rows = SnowflakeValueCheckOperator(
        task_id="check_rows",
        sql=f"SELECT COUNT(*) FROM {TARGET}",
        pass_value=6,
        retry_on_failure=False,
    )

    (
        create_table >> multi_statement >> variable_count >> with_bindings
        >> with_timeout >> check_rows
        >> PythonOperator(
            task_id="report",
            python_callable=report,
            provide_context=True,
        )
    )
