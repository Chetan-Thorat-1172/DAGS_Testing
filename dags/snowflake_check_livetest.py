"""Live end-to-end test for SnowflakeCheckOperator and SnowflakeValueCheckOperator.

Every check here is meant to PASS, so a green run is the assertion. The SQL
references no tables or procedures — only scalar expressions and GENERATOR — so
it runs under whatever role the connection uses, with no grants needed.

The deliberate failures live in snowflake_check_failures.py.

Set CONN below to the Connection ID you created in the UI.
"""

import json
from datetime import datetime

from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
    SQLExecuteQueryOperator,
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
)

CONN = "snowflake_conn"


def verify(**context):
    """Assert what the driver actually returns for a NUMBER column.

    The truthiness rule treats numeric-looking text as a number because a
    Snowflake NUMBER arrives as an int64 on the Arrow result path but as a
    string on the JSON one, and the row carries no type information. This task
    records which one this account negotiates, so the rule is backed by
    evidence rather than by the reading of the driver source.
    """
    ti = context["ti"]
    raw = ti.xcom_pull(task_ids="probe_number_type", key="return_value")
    print(f"probe_number_type -> {raw!r}")
    rows = json.loads(raw) if isinstance(raw, str) else raw

    value = rows[0]["N"]
    print(f"COUNT(*) came back as {type(value).__name__}: {value!r}")

    assert str(value) == "3", f"expected 3, got {value!r}"
    if isinstance(value, str):
        print("RESULT: NUMBER arrives as TEXT — the numeric-text truthiness rule is load-bearing.")
    else:
        print("RESULT: NUMBER arrives as a number — the rule is belt-and-braces here.")

    print("ALL CHECKS ABOVE PASSED (a failed check fails its own task)")
    return "ok"


with DAG(
    dag_id="snowflake_check_livetest",
    description="SnowflakeCheckOperator / SnowflakeValueCheckOperator — passing cases",
    tags=["livetest", "snowflake"],
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args={"snowflake_conn_id": CONN},
) as dag:

    # --- SnowflakeCheckOperator: every value in the first row must be true ---

    check_scalar = SnowflakeCheckOperator(
        task_id="check_scalar",
        sql="SELECT 1",
    )

    # The canonical use: a non-zero count passes.
    check_count = SnowflakeCheckOperator(
        task_id="check_count",
        sql="SELECT COUNT(*) FROM TABLE(GENERATOR(ROWCOUNT => 5))",
    )

    # Every column in the row is evaluated, not just the first.
    check_every_column = SnowflakeCheckOperator(
        task_id="check_every_column",
        sql="SELECT 1 AS A, 'text' AS B, TRUE AS C",
    )

    # Bind parameters reach the check the same way they reach a query.
    check_with_binds = SnowflakeCheckOperator(
        task_id="check_with_binds",
        sql="SELECT COUNT(*) FROM TABLE(GENERATOR(ROWCOUNT => 5)) WHERE :flag = 1",
        parameters={"flag": 1},
    )

    # --- SnowflakeValueCheckOperator: the row must equal pass_value ---

    value_exact = SnowflakeValueCheckOperator(
        task_id="value_exact",
        sql="SELECT 42",
        pass_value=42,
    )

    value_text = SnowflakeValueCheckOperator(
        task_id="value_text",
        sql="SELECT 'OK'",
        pass_value="OK",
    )

    # 102 falls inside 100 ± 5%.
    value_tolerance = SnowflakeValueCheckOperator(
        task_id="value_tolerance",
        sql="SELECT 102",
        pass_value=100,
        tolerance=0.05,
    )

    # Both bounds are inclusive.
    value_tolerance_edge = SnowflakeValueCheckOperator(
        task_id="value_tolerance_edge",
        sql="SELECT 105",
        pass_value=100,
        tolerance=0.05,
    )

    # pass_value=None matches a SQL NULL, the way the reference's str(None) does.
    value_null = SnowflakeValueCheckOperator(
        task_id="value_null",
        sql="SELECT NULL",
        pass_value=None,
    )

    value_boolean = SnowflakeValueCheckOperator(
        task_id="value_boolean",
        sql="SELECT TRUE",
        pass_value=True,
    )

    # A numeric pass_value written as text is still a numeric comparison.
    value_numeric_text = SnowflakeValueCheckOperator(
        task_id="value_numeric_text",
        sql="SELECT 7",
        pass_value="7",
    )

    # Every value in the row is compared, not only the first.
    value_every_column = SnowflakeValueCheckOperator(
        task_id="value_every_column",
        sql="SELECT 3 AS A, 3 AS B, 3 AS C",
        pass_value=3,
    )

    # --- Session overrides reach the check ---

    # Nothing here depends on the warehouse's identity; this only proves the
    # override is accepted and the check still runs.
    check_session_override = SnowflakeCheckOperator(
        task_id="check_session_override",
        sql="SELECT 1",
        hook_params={"warehouse": "COMPUTE_WH"},
    )

    # --- What type does a NUMBER actually arrive as? ---

    probe_number_type = SQLExecuteQueryOperator(
        task_id="probe_number_type",
        sql="SELECT COUNT(*) AS N FROM TABLE(GENERATOR(ROWCOUNT => 3))",
    )

    report = PythonOperator(
        task_id="verify",
        python_callable=verify,
        provide_context=True,
    )

    [
        check_scalar,
        check_count,
        check_every_column,
        check_with_binds,
        value_exact,
        value_text,
        value_tolerance,
        value_tolerance_edge,
        value_null,
        value_boolean,
        value_numeric_text,
        value_every_column,
        check_session_override,
        probe_number_type,
    ] >> report
